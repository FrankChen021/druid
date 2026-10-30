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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NativeSysTasksQueryTest extends EmbeddedClusterTestBase
{
  private static final String TASK_PREFIX = "native_sys_tasks_";
  private static final Set<String> ALL_TASK_IDS = Set.of(
      "native_sys_tasks_a_0",
      "native_sys_tasks_a_1",
      "native_sys_tasks_b_0",
      "native_sys_tasks_b_1",
      "native_sys_tasks_b_2"
  );
  private static final String WEB_CONSOLE_TASKS_SQL = """
      WITH tasks AS (SELECT
        "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg",
        CASE WHEN "error_msg" IN ('Shutdown request from user', 'Canceled: Query canceled by user or by task shutdown.')
        THEN 'CANCELED' WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status"
        FROM sys.tasks
      )
      SELECT "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg", "status"
      FROM tasks
      ORDER BY
        (CASE "status" WHEN 'RUNNING' THEN 4 WHEN 'PENDING' THEN 3 WHEN 'WAITING' THEN 2 ELSE 1 END) DESC,
        "created_time" DESC
      """;

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .addServer(new EmbeddedCoordinator())
                               .addServer(new EmbeddedIndexer()
                                              .addProperty("druid.worker.capacity", "5"))
                               .addServer(overlord)
                               .addServer(broker);
  }

  @BeforeAll
  public void createTasks()
  {
    createTasks("a", "native_sys_a", 2);
    createTasks("b", "native_sys_b", 3);
  }

  @Test
  public void testGroupByUsesOverlordProvider()
  {
    final String result = cluster.runSql(
        """
        SELECT datasource, COUNT(*)
        FROM sys.tasks
        WHERE task_id = 'native_sys_tasks_a_0' AND datasource = 'native_sys_a'
        GROUP BY datasource
        """,
        nativeQueryContext()
    );

    Assertions.assertEquals("native_sys_a,1", result);
  }

  /** A status filter is pushed down to choose which task lookups run: only completed tasks match SUCCESS. */
  @Test
  public void testStatusFilterSelectsTaskLookups()
  {
    Assertions.assertEquals(
        ALL_TASK_IDS,
        taskIds(cluster.runSql(
            "SELECT task_id FROM sys.tasks WHERE status = 'SUCCESS' AND datasource IN ('native_sys_a', 'native_sys_b')",
            nativeQueryContext()
        ))
    );
    Assertions.assertEquals(
        "",
        cluster.runSql(
            "SELECT task_id FROM sys.tasks WHERE status = 'RUNNING' AND datasource IN ('native_sys_a', 'native_sys_b')",
            nativeQueryContext()
        )
    );
  }

  @Test
  public void testWebConsoleTasksQueryUsesOverlordProvider()
  {
    Assertions.assertEquals(ALL_TASK_IDS, taskIds(cluster.runSql(WEB_CONSOLE_TASKS_SQL, nativeQueryContext())));
  }

  /** Native execution returns the same rows as the Broker's SQL interpreter for the web console's tasks query. */
  @Test
  public void testNativeResultsMatchSqlInterpreter()
  {
    final Set<String> nativeRows = Set.of(cluster.runSql(WEB_CONSOLE_TASKS_SQL, nativeQueryContext()).split("\n"));
    final Set<String> sqlRows = Set.of(cluster.runSql(WEB_CONSOLE_TASKS_SQL).split("\n"));
    Assertions.assertEquals(sqlRows, nativeRows);
  }

  private void createTasks(final String suffix, final String datasource, final int count)
  {
    for (int i = 0; i < count; i++) {
      final String taskId = TASK_PREFIX + suffix + "_" + i;
      cluster.callApi().runTask(new NoopTask(taskId, null, datasource, 1L, 0L, null), overlord);
    }
  }

  private static Set<String> taskIds(final String csv)
  {
    return Arrays.stream(csv.split("\n"))
                 .map(row -> row.contains(",") ? row.substring(0, row.indexOf(',')) : row)
                 .collect(Collectors.toSet());
  }

  private static Map<String, Object> nativeQueryContext()
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true
    );
  }
}
