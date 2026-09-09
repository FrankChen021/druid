/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.metadata;

import org.apache.druid.indexer.TaskIdStatus;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.skife.jdbi.v2.PreparedBatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Compares the metadata-read portion of the bindable and native {@code sys.tasks} execution paths.
 *
 * <p>The bindable path requests every task from the Overlord and evaluates the SQL predicate afterward. The native
 * path converts the same predicate to a validated {@link TaskStorageQueryFilter}, applies it in metadata storage, and
 * retains the predicate as a residual filter. This benchmark intentionally excludes SQL planning and HTTP transport
 * so it isolates the benefit of metadata filter pushdown.</p>
 *
 * <p>The synthetic table contains 100,000 migrated task records split evenly between active and completed tasks.
 * Datasource, type, and group values have controlled cardinalities so each query returns a stable number of rows.</p>
 *
 * <p>Query cases:</p>
 * <ul>
 *   <li>The unfiltered task-list query issued by the web console:
 *     <pre>{@code
 *     WITH tasks AS (
 *       SELECT "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg",
 *         CASE
 *           WHEN "error_msg" IN (
 *             'Shutdown request from user',
 *             'Canceled: Query canceled by user or by task shutdown.'
 *           ) THEN 'CANCELED'
 *           WHEN "status" = 'RUNNING' THEN "runner_status"
 *           ELSE "status"
 *         END AS "status"
 *       FROM sys.tasks
 *     )
 *     SELECT "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg", "status"
 *     FROM tasks
 *     ORDER BY
 *       CASE "status"
 *         WHEN 'RUNNING' THEN 4
 *         WHEN 'PENDING' THEN 3
 *         WHEN 'WAITING' THEN 2
 *         ELSE 1
 *       END DESC,
 *       "created_time" DESC
 *     }</pre>
 *   </li>
 *   <li>{@code SELECT * FROM sys.tasks WHERE task_id = 'task-050000'}</li>
 *   <li>{@code SELECT * FROM sys.tasks WHERE datasource = 'datasource-000'}</li>
 *   <li>{@code SELECT * FROM sys.tasks WHERE datasource = 'datasource-000' AND type = 'type-0'}</li>
 *   <li>{@code SELECT * FROM sys.tasks WHERE group_id = 'group-0000'}</li>
 * </ul>
 *
 * <p>The native planner maps {@code task_id} to metadata column {@code id}. The other benchmarked columns retain
 * their names. Equality filters on {@code type} and {@code group_id} are both pushed down, although the current task
 * table schema does not index either column.</p>
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xmx4g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SysTasksTableBenchmark
{
  private static final int NUM_TASKS = 100_000;
  private static final int INSERT_BATCH_SIZE = 5_000;
  private static final String CREATED_TIME = "2025-01-01T00:00:00.000Z";
  private static final String TARGET_TASK_ID = "task-050000";
  private static final String TARGET_DATASOURCE = "datasource-000";
  private static final String TARGET_TYPE = "type-0";
  private static final String TARGET_GROUP_ID = "group-0000";

  public enum QueryCase
  {
    FULL_LIST,
    TASK_ID,
    DATASOURCE,
    DATASOURCE_AND_TYPE,
    GROUP_ID
  }

  @Param
  private QueryCase queryCase;

  private TestDerbyConnector connector;
  private DerbyMetadataStorageActionHandler handler;
  private Map<TaskLookupType, TaskLookup> taskLookups;
  private TaskStorageQueryFilter pushdownFilter;
  private Predicate<TaskIdStatus> residualFilter;

  @Setup(Level.Trial)
  public void setup() throws ClassNotFoundException
  {
    // The benchmark uber-jar has a consolidated JDBC service file that does not list Derby.
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    connector = new TestDerbyConnector();
    connector.createDatabase();

    final MetadataStorageTablesConfig tablesConfig = connector.getMetadataTablesConfig();
    connector.prepareTaskEntryTable(tablesConfig.getTasksTable());
    handler = new DerbyMetadataStorageActionHandler(
        connector,
        new DefaultObjectMapper(),
        tablesConfig.getTasksTable(),
        tablesConfig.getTaskLockTable()
    );
    insertTasks(tablesConfig.getTasksTable());

    taskLookups = Map.of(
        TaskLookupType.ACTIVE,
        ActiveTaskLookup.getInstance(),
        TaskLookupType.COMPLETE,
        CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2020-01-01"))
    );
    pushdownFilter = new TaskStorageQueryFilter(filtersFor(queryCase));
    residualFilter = residualFilterFor(queryCase);
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    connector.tearDown();
  }

  /** Simulates the bindable path: retrieve all tasks, then apply the SQL predicate. */
  @Benchmark
  public void bindable(Blackhole blackhole)
  {
    consumeMatchingRows(handler.getTaskStatusList(taskLookups, null, false), blackhole);
  }

  /** Simulates the native path: push eligible predicates into metadata storage, then apply the residual predicate. */
  @Benchmark
  public void nativeQuery(Blackhole blackhole)
  {
    consumeMatchingRows(handler.getTaskStatusListWithFilter(taskLookups, pushdownFilter, false), blackhole);
  }

  private void consumeMatchingRows(final List<TaskIdStatus> candidates, final Blackhole blackhole)
  {
    long matchedRows = 0;
    for (final TaskIdStatus candidate : candidates) {
      if (residualFilter.test(candidate)) {
        blackhole.consume(candidate);
        matchedRows++;
      }
    }
    blackhole.consume(matchedRows);
  }

  private void insertTasks(final String tasksTable)
  {
    final byte[] payload = new byte[]{'{', '}'};
    final byte[] activeStatus = JacksonUtils.toBytes(new DefaultObjectMapper(), TaskStatus.running("benchmark"));
    final byte[] completeStatus = JacksonUtils.toBytes(new DefaultObjectMapper(), TaskStatus.success("benchmark"));
    final String sql = StringUtils.format(
        "INSERT INTO %s (id, created_date, datasource, payload, status_payload, active, type, group_id) "
        + "VALUES (:id, :created_date, :datasource, :payload, :status_payload, :active, :type, :group_id)",
        tasksTable
    );

    for (int batchStart = 0; batchStart < NUM_TASKS; batchStart += INSERT_BATCH_SIZE) {
      final int start = batchStart;
      final int end = Math.min(batchStart + INSERT_BATCH_SIZE, NUM_TASKS);
      connector.retryWithHandle(handle -> {
        final PreparedBatch batch = handle.prepareBatch(sql);
        for (int i = start; i < end; i++) {
          final boolean active = i % 2 == 0;
          batch.add()
               .bind("id", taskId(i))
               .bind("created_date", CREATED_TIME)
               .bind("datasource", dataSource(i))
               .bind("payload", payload)
               .bind("status_payload", active ? activeStatus : completeStatus)
               .bind("active", active)
               .bind("type", type(i))
               .bind("group_id", groupId(i));
        }
        batch.execute();
        return null;
      });
    }
  }

  private static List<DimFilter> filtersFor(final QueryCase query)
  {
    return switch (query) {
      case FULL_LIST -> List.of();
      case TASK_ID -> List.of(equality("id", TARGET_TASK_ID));
      case DATASOURCE -> List.of(equality("datasource", TARGET_DATASOURCE));
      case DATASOURCE_AND_TYPE -> List.of(
          equality("datasource", TARGET_DATASOURCE),
          equality("type", TARGET_TYPE)
      );
      case GROUP_ID -> List.of(equality("group_id", TARGET_GROUP_ID));
    };
  }

  private static Predicate<TaskIdStatus> residualFilterFor(final QueryCase query)
  {
    return switch (query) {
      case FULL_LIST -> ignored -> true;
      case TASK_ID -> task -> TARGET_TASK_ID.equals(task.getTaskIdentifier().getId());
      case DATASOURCE -> task -> TARGET_DATASOURCE.equals(task.getDataSource());
      case DATASOURCE_AND_TYPE -> task -> TARGET_DATASOURCE.equals(task.getDataSource())
                                         && TARGET_TYPE.equals(task.getTaskIdentifier().getType());
      case GROUP_ID -> task -> TARGET_GROUP_ID.equals(task.getTaskIdentifier().getGroupId());
    };
  }

  private static EqualityFilter equality(final String column, final String value)
  {
    return new EqualityFilter(column, ColumnType.STRING, value, null);
  }

  private static String taskId(final int taskNumber)
  {
    return StringUtils.format("task-%06d", taskNumber);
  }

  private static String dataSource(final int taskNumber)
  {
    return StringUtils.format("datasource-%03d", taskNumber % 100);
  }

  private static String type(final int taskNumber)
  {
    return StringUtils.format("type-%d", (taskNumber / 100) % 10);
  }

  private static String groupId(final int taskNumber)
  {
    return StringUtils.format("group-%04d", taskNumber % 1_000);
  }
}
