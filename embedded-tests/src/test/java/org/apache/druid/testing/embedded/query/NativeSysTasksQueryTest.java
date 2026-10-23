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
import org.apache.druid.query.QueryContexts;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NativeSysTasksQueryTest extends EmbeddedClusterTestBase
{
  private static final String TASK_PREFIX = "native_sys_mvp_";

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
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

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testGroupByUsesOverlordProvider(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT datasource, COUNT(*)
        FROM sys.tasks
        WHERE task_id = 'native_sys_mvp_a_0' AND datasource = 'native_sys_a'
        GROUP BY datasource
        """,
        nativeQueryContext(plannerStrategy)
    );

    final Set<String> rows = Arrays.stream(result.split("\\n")).collect(Collectors.toSet());
    Assertions.assertEquals(Set.of("native_sys_a,1"), rows);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeAggregationsSupportDistinctCount(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*), COUNT(DISTINCT task_id), COUNT(DISTINCT datasource), SUM(1)
        FROM sys.tasks
        WHERE datasource IN ('native_sys_a', 'native_sys_b')
        """,
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("5,5,2,5", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNestedTaskAggregation(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*)
        FROM (SELECT task_id, COUNT(*) AS task_count
              FROM sys.tasks
              WHERE datasource IN ('native_sys_a', 'native_sys_b')
              GROUP BY task_id)
        WHERE task_count > 0
        """,
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("5", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testOuterAggregationOverTaskSubquery(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT SUM(task_count)
        FROM (SELECT datasource, COUNT(*) AS task_count
              FROM sys.tasks
              WHERE datasource IN ('native_sys_a', 'native_sys_b')
              GROUP BY datasource)
        """,
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("5", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testInAndOrFilters(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    assertTaskFilterQuery(
        "task_id IN ('native_sys_mvp_a_0', 'native_sys_mvp_b_2') "
        + "OR task_id = 'native_sys_mvp_missing'",
        Set.of("native_sys_mvp_a_0", "native_sys_mvp_b_2"),
        plannerStrategy
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testRangeAndLikeFilters(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    assertTaskFilterQuery(
        "task_id >= 'native_sys_mvp_b_0' "
        + "AND task_id < 'native_sys_mvp_b_2' "
        + "AND task_id LIKE 'native_sys_mvp_b_%'",
        Set.of("native_sys_mvp_b_0", "native_sys_mvp_b_1"),
        plannerStrategy
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testExpressionFilterPreservesVirtualColumn(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    assertTaskFilterQuery(
        "UPPER(task_id) = 'NATIVE_SYS_MVP_A_0'",
        Set.of("native_sys_mvp_a_0"),
        plannerStrategy
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNegatedLikeFilter(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    assertTaskFilterQuery(
        "datasource = 'native_sys_b' AND task_id NOT LIKE '%b_2'",
        Set.of("native_sys_mvp_b_0", "native_sys_mvp_b_1"),
        plannerStrategy
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testStatusLikeFilterRemainsForResidualEvaluation(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    assertTaskFilterQuery(
        "task_id LIKE 'native_sys_mvp_%' AND status LIKE '%'",
        Set.of(
            "native_sys_mvp_a_0",
            "native_sys_mvp_a_1",
            "native_sys_mvp_b_0",
            "native_sys_mvp_b_1",
            "native_sys_mvp_b_2"
        ),
        plannerStrategy
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testWebConsoleTasksQueryUsesOverlordProvider(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
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
        """,
        nativeQueryContext(plannerStrategy)
    );

    final Set<String> taskIds = Arrays.stream(result.split("\\n"))
                                      .map(row -> row.substring(0, row.indexOf(',')))
                                      .collect(Collectors.toSet());
    Assertions.assertEquals(
        Set.of(
            "native_sys_mvp_a_0",
            "native_sys_mvp_a_1",
            "native_sys_mvp_b_0",
            "native_sys_mvp_b_1",
            "native_sys_mvp_b_2"
        ),
        taskIds
    );
  }

  private void createTasks(final String suffix, final String datasource, final int count)
  {
    for (int i = 0; i < count; i++) {
      final String taskId = TASK_PREFIX + suffix + "_" + i;
      cluster.callApi().runTask(new NoopTask(taskId, null, datasource, 1L, 0L, null), overlord);
    }
  }

  private void assertTaskFilterQuery(
      final String whereClause,
      final Set<String> expectedTaskIds,
      final QueryContexts.PlannerStrategy plannerStrategy
  )
  {
    // The SQL is a format string, so pass the WHERE clause as an argument to keep its LIKE wildcards intact.
    final String result = cluster.runSql(
        "SELECT task_id FROM sys.tasks WHERE %s",
        nativeQueryContext(plannerStrategy),
        whereClause
    );

    Assertions.assertEquals(expectedTaskIds, Arrays.stream(result.split("\\n")).collect(Collectors.toSet()));
  }

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.ALLOW_NATIVE_FUNCTIONS_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
