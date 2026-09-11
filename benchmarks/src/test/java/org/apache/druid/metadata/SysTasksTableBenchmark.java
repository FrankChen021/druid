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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskIdStatus;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.schema.DruidSchemaProvider;
import org.apache.druid.sql.calcite.schema.LookupSchema;
import org.apache.druid.sql.calcite.schema.SystemSchemaProvider;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.hook.DruidHookDispatcher;
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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes SQL against 100,000 {@code sys.tasks} rows and compares the Bindable and native planning paths.
 *
 * <p>Both paths execute the complete SQL statement, including planning, expressions, aggregation, sorting, and result
 * materialization. The native path runs the Broker query over an in-process system-table handler so this benchmark
 * excludes only node discovery and HTTP transport. Its task provider reads the same Derby metadata table as the
 * Bindable Overlord client and applies the production metadata predicate builder.</p>
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xmx4g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SqlTestFrameworkConfig.ComponentSupplier(SysTasksTableBenchmark.BenchmarkComponentSupplier.class)
public class SysTasksTableBenchmark extends BaseCalciteQueryTest
{
  private static final int NUM_TASKS = 100_000;
  private static final int INSERT_BATCH_SIZE = 5_000;
  private static final String CREATED_TIME = "2025-01-01T00:00:00.000Z";
  private static final String TARGET_TASK_ID = "task-050000";
  private static final String TARGET_DATASOURCE = "datasource-000";
  private static final String TARGET_TYPE = "type-0";
  private static final String TARGET_GROUP_ID = "group-0000";

  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("task_id", "id"),
      new SystemTablePushdownFilter("group_id", null),
      new SystemTablePushdownFilter("type", null),
      new SystemTablePushdownFilter("datasource", null),
      new SystemTablePushdownFilter("created_time", "created_date"),
      new SystemTablePushdownFilter("status", null)
  );

  private static final String WEB_CONSOLE_TASKS_SQL =
      "WITH tasks AS (SELECT\n"
      + "  \"task_id\", \"group_id\", \"type\", \"datasource\", \"created_time\", \"location\", "
      + "\"duration\", \"error_msg\",\n"
      + "  CASE WHEN \"error_msg\" IN ('Shutdown request from user', "
      + "'Canceled: Query canceled by user or by task shutdown.') THEN 'CANCELED' "
      + "WHEN \"status\" = 'RUNNING' THEN \"runner_status\" ELSE \"status\" END AS \"status\"\n"
      + "  FROM sys.tasks\n"
      + ")\n"
      + "SELECT \"task_id\", \"group_id\", \"type\", \"datasource\", \"created_time\", \"location\", "
      + "\"duration\", \"error_msg\", \"status\"\n"
      + "FROM tasks\n"
      + "ORDER BY\n"
      + "  (CASE \"status\" WHEN 'RUNNING' THEN 4 WHEN 'PENDING' THEN 3 WHEN 'WAITING' THEN 2 ELSE 1 END) DESC,\n"
      + "  \"created_time\" DESC";

  private static BenchmarkTaskData taskData;

  @Param({"false", "true"})
  private boolean useNativeQueryForSystemTables;

  private TestDerbyConnector connector;
  private SqlStatementFactory statementFactory;
  private Map<String, Object> queryContext;

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    connector = new TestDerbyConnector();
    connector.createDatabase();

    final MetadataStorageTablesConfig tablesConfig = connector.getMetadataTablesConfig();
    connector.prepareTaskEntryTable(tablesConfig.getTasksTable());
    final DerbyMetadataStorageActionHandler handler = new DerbyMetadataStorageActionHandler(
        connector,
        new DefaultObjectMapper(),
        tablesConfig.getTasksTable(),
        tablesConfig.getTaskLockTable()
    );
    insertTasks(tablesConfig.getTasksTable());
    taskData = new BenchmarkTaskData(handler);

    queryFrameworkRule.setConfig(
        new SqlTestFrameworkConfig(Arrays.asList(SysTasksTableBenchmark.class.getAnnotations()))
    );
    final SqlTestFramework framework = queryFramework();
    statementFactory = createStatementFactory(framework, taskData.createOverlordClient());
    queryContext = ImmutableMap.of(
        PlannerContext.CTX_USE_NATIVE_QUERY_FOR_SYSTEM_TABLES,
        useNativeQueryForSystemTables
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    queryFramework().close();
    taskData = null;
    connector.tearDown();
  }

  /** Executes the exact task-list query currently issued by the Web Console. */
  @Benchmark
  public void webConsoleFullList(final Blackhole blackhole)
  {
    executeSql(WEB_CONSOLE_TASKS_SQL, blackhole);
  }

  /** {@code SELECT * FROM sys.tasks WHERE task_id = 'task-050000'} */
  @Benchmark
  public void taskIdFilter(final Blackhole blackhole)
  {
    executeSql("SELECT * FROM sys.tasks WHERE task_id = '" + TARGET_TASK_ID + "'", blackhole);
  }

  /** {@code SELECT * FROM sys.tasks WHERE datasource = 'datasource-000'} */
  @Benchmark
  public void dataSourceFilter(final Blackhole blackhole)
  {
    executeSql("SELECT * FROM sys.tasks WHERE datasource = '" + TARGET_DATASOURCE + "'", blackhole);
  }

  /** {@code SELECT * FROM sys.tasks WHERE datasource = 'datasource-000' AND type = 'type-0'} */
  @Benchmark
  public void dataSourceAndTypeFilter(final Blackhole blackhole)
  {
    executeSql(
        "SELECT * FROM sys.tasks WHERE datasource = '" + TARGET_DATASOURCE + "' AND type = '" + TARGET_TYPE + "'",
        blackhole
    );
  }

  /** {@code SELECT * FROM sys.tasks WHERE group_id = 'group-0000'} */
  @Benchmark
  public void groupIdFilter(final Blackhole blackhole)
  {
    executeSql("SELECT * FROM sys.tasks WHERE group_id = '" + TARGET_GROUP_ID + "'", blackhole);
  }

  /** {@code SELECT CASE WHEN status = 'RUNNING' THEN runner_status ELSE status END, COUNT(*) FROM sys.tasks GROUP BY 1} */
  @Benchmark
  public void groupByEffectiveStatus(final Blackhole blackhole)
  {
    executeSql(
        "SELECT CASE WHEN status = 'RUNNING' THEN runner_status ELSE status END AS effective_status, COUNT(*) "
        + "FROM sys.tasks GROUP BY 1",
        blackhole
    );
  }

  /** {@code SELECT datasource, COUNT(*) FROM sys.tasks GROUP BY datasource} */
  @Benchmark
  public void groupByDataSource(final Blackhole blackhole)
  {
    executeSql("SELECT datasource, COUNT(*) FROM sys.tasks GROUP BY datasource", blackhole);
  }

  /** {@code SELECT type, COUNT(*) FROM sys.tasks WHERE datasource = 'datasource-000' GROUP BY type} */
  @Benchmark
  public void groupByTypeForDataSource(final Blackhole blackhole)
  {
    executeSql(
        "SELECT type, COUNT(*) FROM sys.tasks WHERE datasource = '" + TARGET_DATASOURCE + "' GROUP BY type",
        blackhole
    );
  }

  private void executeSql(final String sql, final Blackhole blackhole)
  {
    final SqlQueryPlus sqlQuery = SqlQueryPlus.builder(sql)
                                              .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
                                              .systemDefaultContext(Map.of())
                                              .queryContext(queryContext)
                                              .build();
    try (DirectStatement statement = statementFactory.directStatement(sqlQuery)) {
      blackhole.consume(statement.execute().getResults().toList());
    }
  }

  private SqlStatementFactory createStatementFactory(
      final SqlTestFramework framework,
      final OverlordClient overlordClient
  )
  {
    final PlannerConfig plannerConfig = PLANNER_CONFIG_DEFAULT;
    final DruidSchemaProvider druidSchemaProvider = framework.injector().getInstance(DruidSchemaProvider.class);
    final SystemSchemaProvider systemSchemaProvider = CalciteTests.createMockSystemSchemaProvider(
        druidSchemaProvider.getSegmentMetadataCache(),
        framework.injector().getInstance(org.apache.druid.client.TimelineServerView.class),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        plannerConfig,
        overlordClient
    );
    final DruidSchemaCatalogProvider schemaProvider = QueryFrameworkUtils.createMockRootSchemaProvider(
        framework.injector().getInstance(ViewManager.class),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        druidSchemaProvider,
        systemSchemaProvider,
        framework.injector().getInstance(LookupSchema.class),
        framework.operatorTable(),
        plannerConfig
    );
    final SqlTestFramework.QueryComponentSupplier componentSupplier = framework.injector().getInstance(
        SqlTestFramework.QueryComponentSupplier.class
    );
    final PlannerFactory plannerFactory = new PlannerFactory(
        schemaProvider,
        framework.operatorTable(),
        framework.macroTable(),
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        framework.queryJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(componentSupplier.getPlannerComponentSupplier().extensionCalciteRules()),
        framework.injector().getInstance(JoinableFactoryWrapper.class),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new DruidHookDispatcher()
    );
    return QueryFrameworkUtils.createSqlMultiStatementFactory(framework.engine(), plannerFactory);
  }

  private void insertTasks(final String tasksTable)
  {
    final byte[] payload = new byte[]{'{', '}'};
    final ObjectMapper mapper = new DefaultObjectMapper();
    final byte[] activeStatus = JacksonUtils.toBytes(mapper, TaskStatus.running("benchmark"));
    final byte[] completeStatus = JacksonUtils.toBytes(mapper, TaskStatus.success("benchmark"));
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

  public static class BenchmarkComponentSupplier extends StandardComponentSupplier
  {
    public BenchmarkComponentSupplier(final TempDirProducer tempDirProducer)
    {
      super(tempDirProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(
          super.getCoreModule(),
          binder -> DruidBinders.dataSourceQueryHandlerBinder(binder)
                                .addBinding(SystemTableDataSource.class)
                                .to(BenchmarkSystemTableQueryHandler.class)
      );
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(
        final SpecificSegmentsQuerySegmentWalker walker,
        final ObjectMapper jsonMapper
    )
    {
      return walker;
    }
  }

  public static class BenchmarkSystemTableQueryHandler implements DataSourceQueryHandler
  {
    private final QuerySegmentWalker walker;

    @com.google.inject.Inject
    public BenchmarkSystemTableQueryHandler(final QuerySegmentWalker walker)
    {
      this.walker = walker;
    }

    @Override
    public <T> QueryRunner<T> createRunner(
        final Query<T> query,
        final AuthenticationResult authenticationResult,
        final boolean executeLocally
    )
    {
      final BenchmarkTaskData currentTaskData = taskData;
      if (currentTaskData == null) {
        throw DruidException.defensive("Benchmark task data has not been initialized");
      }
      final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
          currentTaskData.getNativeRows(query),
          TaskTableDescriptor.ROW_SIGNATURE
      );
      final ScanQuery scanQuery = Druids.newScanQueryBuilder()
                                        .dataSource(inlineDataSource)
                                        .eternityInterval()
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .limit(Long.MAX_VALUE)
                                        .columns(TaskTableDescriptor.ROW_SIGNATURE)
                                        .build();
      final Query<T> resolvedQuery = query.withDataSource(
          replaceSystemTable(query.getDataSource(), new QueryDataSource(scanQuery))
      );
      final QueryRunner<T> runner = resolvedQuery.getRunner(walker);
      return (queryPlus, responseContext) -> runner.run(queryPlus.withQuery(resolvedQuery), responseContext);
    }

    private static DataSource replaceSystemTable(
        final DataSource dataSource,
        final DataSource replacementDataSource
    )
    {
      if (dataSource instanceof SystemTableDataSource) {
        return replacementDataSource;
      }
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(child -> replaceSystemTable(child, replacementDataSource))
                    .toList()
      );
    }
  }

  private static class BenchmarkTaskData
  {
    private final DerbyMetadataStorageActionHandler handler;
    private final Map<TaskLookupType, TaskLookup> taskLookups = Map.of(
        TaskLookupType.ACTIVE,
        ActiveTaskLookup.getInstance(),
        TaskLookupType.COMPLETE,
        CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2020-01-01"))
    );

    private BenchmarkTaskData(final DerbyMetadataStorageActionHandler handler)
    {
      this.handler = handler;
    }

    private OverlordClient createOverlordClient()
    {
      return new NoopOverlordClient()
      {
        @Override
        public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
            @Nullable final String state,
            @Nullable final String dataSource,
            @Nullable final Integer maxCompletedTasks
        )
        {
          return Futures.immediateFuture(
              CloseableIterators.withEmptyBaggage(
                  handler.getTaskStatusList(taskLookups, null, false)
                         .stream()
                         .map(BenchmarkTaskData::toTaskStatusPlus)
                         .iterator()
              )
          );
        }
      };
    }

    private Iterable<Object[]> getNativeRows(final Query<?> query)
    {
      final TaskStorageQueryFilter filter = new TaskStorageQueryFilter(
          SystemTablePushdownFilter.extract(query, PUSHDOWN_FILTERS)
      );
      return () -> handler.getTaskStatusListWithFilter(taskLookups, filter, false)
                         .stream()
                         .map(BenchmarkTaskData::toRow)
                         .iterator();
    }

    private static TaskStatusPlus toTaskStatusPlus(final TaskIdStatus task)
    {
      final TaskStatus status = task.getStatus();
      final RunnerTaskState runnerState = status.isRunnable() ? RunnerTaskState.RUNNING : RunnerTaskState.NONE;
      return new TaskStatusPlus(
          task.getTaskIdentifier().getId(),
          task.getTaskIdentifier().getGroupId(),
          task.getTaskIdentifier().getType(),
          task.getCreatedTime(),
          task.getCreatedTime(),
          status.getStatusCode(),
          runnerState,
          status.getDuration(),
          status.getLocation(),
          task.getDataSource(),
          status.getErrorMsg()
      );
    }

    private static Object[] toRow(final TaskIdStatus task)
    {
      final TaskStatusPlus status = toTaskStatusPlus(task);
      final TaskLocation location = status.getLocation();
      return new Object[]{
          status.getId(),
          status.getGroupId(),
          status.getType(),
          status.getDataSource(),
          status.getCreatedTime().toString(),
          status.getQueueInsertionTime().toString(),
          status.getStatusCode().toString(),
          status.getRunnerStatusCode().toString(),
          status.getDuration(),
          location.getLocation(),
          location.getHost(),
          (long) location.getPort(),
          (long) location.getTlsPort(),
          status.getErrorMsg()
      };
    }
  }
}
