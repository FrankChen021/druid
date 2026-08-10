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
import org.apache.druid.sql.calcite.planner.PlannerConfig;
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
  private static final String TASK_PREFIX = "native_sys_mvp_";
  private static final Map<String, Object> NATIVE_QUERY_CONTEXT =
      Map.of(PlannerConfig.CTX_ENABLE_NATIVE_QUERY_FOR_SYSTEM_TABLES, true);

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

  @Test
  public void testGroupByUsesOverlordProvider()
  {
    overlord.latchableEmitter().flush();

    final String result = cluster.runSql(
        "SELECT datasource, COUNT(*) "
        + "FROM sys.tasks "
        + "WHERE task_id = 'native_sys_mvp_a_0' AND datasource = 'native_sys_a' "
        + "GROUP BY datasource",
        NATIVE_QUERY_CONTEXT
    );

    final Set<String> rows = Arrays.stream(result.split("\\n")).collect(Collectors.toSet());
    Assertions.assertEquals(Set.of("native_sys_a,1"), rows);

    Assertions.assertEquals(
        1L,
        overlord.latchableEmitter().getLatestMetricEventValue("query/systemTasks/rowsRead").longValue()
    );
    Assertions.assertEquals(
        1L,
        overlord.latchableEmitter().getLatestMetricEventValue("query/systemTasks/rowsReturned").longValue()
    );
  }

  @Test
  public void testWebConsoleTasksQueryUsesOverlordProvider()
  {
    overlord.latchableEmitter().flush();

    final String result = cluster.runSql(
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
        + "  \"created_time\" DESC",
        NATIVE_QUERY_CONTEXT
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
    Assertions.assertEquals(
        5L,
        overlord.latchableEmitter().getLatestMetricEventValue("query/systemTasks/rowsRead").longValue()
    );
    Assertions.assertEquals(
        5L,
        overlord.latchableEmitter().getLatestMetricEventValue("query/systemTasks/rowsReturned").longValue()
    );
  }

  private void createTasks(final String suffix, final String datasource, final int count)
  {
    for (int i = 0; i < count; i++) {
      final String taskId = TASK_PREFIX + suffix + "_" + i;
      cluster.callApi().runTask(new NoopTask(taskId, null, datasource, 1L, 0L, null), overlord);
    }
  }
}
