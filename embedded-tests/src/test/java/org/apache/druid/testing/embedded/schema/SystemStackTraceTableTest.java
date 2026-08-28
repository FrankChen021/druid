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

package org.apache.druid.testing.embedded.schema;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.system.table.StackTraceTableDataProvider;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class SystemStackTraceTableTest extends EmbeddedClusterTestBase
{
  private static final String BROKER_PORT = "9082";
  private static final String BROKER_SERVICE = "test/broker";
  private static final String OVERLORD_PORT = "9090";
  private static final String OVERLORD_SERVICE = "test/overlord";
  private static final String COORDINATOR_PORT = "9081";
  private static final String COORDINATOR_SERVICE = "test/coordinator";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", BROKER_SERVICE)
      .addProperty("druid.plaintextPort", BROKER_PORT);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.service", OVERLORD_SERVICE)
      .addProperty("druid.plaintextPort", OVERLORD_PORT);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.service", COORDINATOR_SERVICE)
      .addProperty("druid.plaintextPort", COORDINATOR_PORT);

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void test_stackTraceTable(final String plannerStrategy)
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final String result = cluster.runSql(
        "SELECT server, service_name, node_roles, collected_at, thread_id, "
        + "thread_state, daemon, priority, cpu_time_ns, user_cpu_time_ns, is_deadlocked, error_message "
        + "FROM sys.stack_trace WHERE server = '%s' AND service_name = '%s'",
        nativeQueryContext(plannerStrategy),
        brokerHost,
        BROKER_SERVICE
    );

    Assertions.assertFalse(result.isEmpty(), "The stack trace table should return broker threads");
    for (final String row : result.split("\\n")) {
      final String[] columns = row.split(",", -1);
      Assertions.assertEquals(brokerHost, columns[0]);
      Assertions.assertEquals(BROKER_SERVICE, columns[1]);
      Assertions.assertEquals("broker", columns[2]);
      Assertions.assertFalse(columns[3].isEmpty());
      assertLong(columns[4]);
      Assertions.assertFalse(columns[5].isEmpty());
      Assertions.assertTrue(columns[6].equals("0") || columns[6].equals("1"), row);
      assertLong(columns[7]);
      if (!columns[8].isEmpty()) {
        assertLong(columns[8]);
      }
      if (!columns[9].isEmpty()) {
        assertLong(columns[9]);
      }
      Assertions.assertTrue(columns[10].equals("0") || columns[10].equals("1"), row);
      Assertions.assertTrue(columns[11].isEmpty());
    }

    Assertions.assertFalse(
        cluster.runSql(
            "SELECT thread_name FROM sys.stack_trace WHERE server = '%s' LIMIT 1",
            nativeQueryContext(plannerStrategy),
            brokerHost
        ).isEmpty()
    );
    Assertions.assertFalse(
        cluster.runSql(
            "SELECT server FROM sys.stack_trace WHERE server = '%s' AND node_roles = 'broker' LIMIT 1",
            nativeQueryContext(plannerStrategy),
            brokerHost
        ).isEmpty()
    );
    Assertions.assertFalse(
        cluster.runSql(
            "SELECT stack FROM sys.stack_trace WHERE server = '%s' LIMIT 1",
            nativeQueryContext(plannerStrategy),
            brokerHost
        ).isEmpty()
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void test_stackTraceTableWithMaxStackTraceFrameDepth(final String plannerStrategy)
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final Map<String, Object> queryContext = new HashMap<>(nativeQueryContext(plannerStrategy));
    queryContext.put(StackTraceTableDataProvider.MAX_STACK_TRACE_FRAME_DEPTH_KEY, 10.9);

    final String result = cluster.runSql(
        "SELECT stack FROM sys.stack_trace WHERE server = '%s' LIMIT 1",
        queryContext,
        brokerHost
    );

    Assertions.assertFalse(result.isEmpty());
    Assertions.assertTrue(countStackFrames(result) <= 10);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void test_stackTraceTableRequiresServerFilter(final String plannerStrategy)
  {
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            "SELECT COUNT(*) FROM sys.stack_trace",
            nativeQueryContext(plannerStrategy)
        )
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(exception.getMessage().contains("requires a filter on the server column"));
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void test_stackTraceTableInFilter(final String plannerStrategy)
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final String coordinatorHost = StringUtils.format("localhost:%s", COORDINATOR_PORT);
    final String result = cluster.runSql(
        "SELECT DISTINCT server FROM sys.stack_trace WHERE server IN ('%s', '%s')",
        nativeQueryContext(plannerStrategy),
        brokerHost,
        coordinatorHost
    );

    Assertions.assertTrue(result.contains(brokerHost));
    Assertions.assertTrue(result.contains(coordinatorHost));
    Assertions.assertFalse(result.contains(StringUtils.format("localhost:%s", OVERLORD_PORT)));
  }

  private static long countStackFrames(final String stackTrace)
  {
    return stackTrace.lines().filter(line -> line.startsWith("\tat ")).count();
  }

  private static void assertLong(final String value)
  {
    try {
      Long.parseLong(value);
    }
    catch (NumberFormatException e) {
      Assertions.fail("Expected a long value but got[" + value + "]", e);
    }
  }

  private static Map<String, Object> nativeQueryContext(final String plannerStrategy)
  {
    return Map.of(
        QueryContexts.ENGINE,
        NativeSqlEngine.NAME,
        PlannerContext.CTX_USE_NATIVE_QUERY_FOR_SYSTEM_TABLES,
        true,
        QueryContexts.CTX_NATIVE_QUERY_SQL_PLANNING_MODE,
        plannerStrategy
    );
  }
}
