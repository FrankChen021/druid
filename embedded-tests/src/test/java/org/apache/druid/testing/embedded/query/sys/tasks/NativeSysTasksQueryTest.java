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

package org.apache.druid.testing.embedded.query.sys.tasks;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.system.table.TaskTableDescriptor;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.ws.rs.core.MediaType;

import java.util.Arrays;
import java.util.List;
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

  /**
   * Two Overlords, so that only one of them is the leader that serves {@code sys.tasks}.
   */
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedOverlord secondOverlord = new EmbeddedOverlord().addProperty("druid.plaintextPort", "7090");
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
                               .addServer(secondOverlord)
                               .addServer(broker);
  }

  @BeforeAll
  public void createTasks() throws InterruptedException
  {
    waitForOverlordLeader();
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

  /**
   * A status filter is pushed down to choose which task lookups run: only completed tasks match SUCCESS.
   */
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

  /**
   * Native execution returns the same rows as the Broker's SQL interpreter for the web console's tasks query.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeResultsMatchSqlInterpreter(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final Map<String, Object> context = nativeQueryContext(plannerStrategy);
    if (plannerStrategy == QueryContexts.PlannerStrategy.COUPLED) {
      // Only the coupled planner explains the native query, which names the system table it reads.
      final String plan = cluster.runSql("EXPLAIN PLAN FOR " + WEB_CONSOLE_TASKS_SQL, context);
      Assertions.assertTrue(plan.contains("systemTable"), plan);
    }

    final List<String> nativeRows =
        Arrays.stream(cluster.runSql(WEB_CONSOLE_TASKS_SQL, context).split("\n")).sorted().toList();
    final List<String> sqlRows = Arrays.stream(
        cluster.runSql(
            WEB_CONSOLE_TASKS_SQL,
            QueryContext.ofMap(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES, false)
        ).split("\n")
    ).sorted().toList();
    Assertions.assertEquals(sqlRows, nativeRows);
  }

  /**
   * SQL reads sys.tasks from the leader Overlord only. Reading the standby as well would fail the query, since the
   * standby rejects the read, or would repeat every task.
   */
  @Test
  public void testSqlReadsTasksFromLeaderOverlordOnly()
  {
    Assertions.assertEquals(
        String.valueOf(ALL_TASK_IDS.size()),
        // The SQL is a format string, so pass the LIKE pattern as an argument to keep its wildcard intact.
        cluster.runSql("SELECT COUNT(*) FROM sys.tasks WHERE task_id LIKE '%s'", nativeQueryContext(), TASK_PREFIX + "%")
    );
  }

  /**
   * A native query sent to an Overlord reads only that Overlord's rows, so the standby rejects it rather than return
   * a table it doesn't own, and the leader serves it.
   */
  @Test
  public void testNativeQueryIsServedByLeaderOverlordOnly() throws Exception
  {
    Assertions.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, postTasksScan(standby()).getStatus());

    final StatusResponseHolder leaderResponse = postTasksScan(leader());
    Assertions.assertEquals(HttpResponseStatus.OK, leaderResponse.getStatus(), leaderResponse.getContent());
    Assertions.assertTrue(leaderResponse.getContent().contains("native_sys_tasks_a_0"), leaderResponse.getContent());
  }

  private StatusResponseHolder postTasksScan(final EmbeddedOverlord target) throws Exception
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(TaskTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .columns("task_id")
                                  .build();
    final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(target) + "/druid/v2/"))
        .setContent(MediaType.APPLICATION_JSON, target.bindings().jsonMapper().writeValueAsBytes(query));
    return target.bindings().globalHttpClient().go(request, StatusResponseHandler.getInstance()).get();
  }

  /**
   * Waits until one of the Overlords is the leader, since an election may still be running when the cluster starts.
   */
  private void waitForOverlordLeader() throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!isLeader(overlord) && !isLeader(secondOverlord)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("No Overlord became the leader");
      }
      Thread.sleep(100);
    }
  }

  private static boolean isLeader(final EmbeddedOverlord candidate)
  {
    return candidate.bindings().overlordLeaderSelector().isLeader();
  }

  private EmbeddedOverlord leader()
  {
    return isLeader(overlord) ? overlord : secondOverlord;
  }

  private EmbeddedOverlord standby()
  {
    return leader() == overlord ? secondOverlord : overlord;
  }

  private void createTasks(final String suffix, final String datasource, final int count)
  {
    for (int i = 0; i < count; i++) {
      final String taskId = TASK_PREFIX + suffix + "_" + i;
      cluster.callApi().runTask(new NoopTask(taskId, null, datasource, 1L, 0L, null), leader());
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

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
