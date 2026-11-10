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

package org.apache.druid.testing.embedded.query.sys.stack_trace;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class NativeSysStackTraceQueryTest extends EmbeddedClusterTestBase
{
  private static final String BROKER_SERVICE = "test/broker";
  private static final String OVERLORD_SERVICE = "test/overlord";
  private static final String COORDINATOR_SERVICE = "test/coordinator";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", BROKER_SERVICE);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.service", OVERLORD_SERVICE);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.service", COORDINATOR_SERVICE);

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTable(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String brokerHost = host(broker);
    final String result = cluster.runSql(
        """
        SELECT server, service_name, node_roles, collected_at, thread_id,
          thread_state, daemon, priority, cpu_time_ns, user_cpu_time_ns, is_deadlocked, error_message
        FROM sys.stack_trace
        WHERE server = '%s' AND service_name = '%s'
        """,
        nativeQueryContext(plannerStrategy),
        brokerHost,
        BROKER_SERVICE
    );

    Assertions.assertFalse(result.isEmpty(), "The stack trace table should return broker threads");
    for (final String row : result.split("\\n")) {
      final String[] columns = row.split(",", -1);
      Assertions.assertEquals(brokerHost, columns[0]);
      Assertions.assertEquals(BROKER_SERVICE, columns[1]);
      Assertions.assertEquals("[broker]", columns[2]);
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
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableRequiresServerFilter(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            """
            SELECT COUNT(*)
            FROM sys.stack_trace
            """,
            nativeQueryContext(plannerStrategy)
        )
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(
        exception.getMessage().contains("using '=' or 'IN' that applies directly to its scan"),
        exception.getMessage()
    );
  }

  /**
   * Without {@code useDruidEngineForSystemTables}, {@code sys.stack_trace} has no interpreter fallback, so the query
   * is rejected with a user error instead of failing during planning.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableRequiresDruidEngine(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            """
            SELECT thread_name
            FROM sys.stack_trace
            WHERE server = '%s'
            """,
            QueryContext.ofMap(
                QueryContextParameters.ENGINE,
                NativeSqlEngine.NAME,
                QueryContextParameters.PLANNER_STRATEGY,
                plannerStrategy
            ),
            host(broker)
        )
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(
        exception.getMessage().contains(
            "sys.stack_trace can only be queried with the Druid engine: set the query context parameter "
            + "useDruidEngineForSystemTables to true and use the native SQL engine."
        ),
        exception.getMessage()
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableInFilter(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String brokerHost = host(broker);
    final String coordinatorHost = host(coordinator);
    final String result = cluster.runSql(
        """
        SELECT DISTINCT server
        FROM sys.stack_trace
        WHERE server IN ('%s', '%s')
        """,
        nativeQueryContext(plannerStrategy),
        brokerHost,
        coordinatorHost
    );

    Assertions.assertTrue(result.contains(brokerHost));
    Assertions.assertTrue(result.contains(coordinatorHost));
    Assertions.assertFalse(result.contains(host(overlord)));
  }

  /**
   * The Broker queries only the servers that the filter on {@code server} allows, and the result holds the rows of
   * exactly those servers. A value that matches no server returns no rows.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableServerFilterReturnsOnlyMatchingServers(
      final QueryContexts.PlannerStrategy plannerStrategy
  )
  {
    final String brokerHost = host(broker);
    final String coordinatorHost = host(coordinator);

    Assertions.assertEquals(
        brokerHost,
        cluster.runSql(
            "SELECT DISTINCT server FROM sys.stack_trace WHERE server = '%s'",
            nativeQueryContext(plannerStrategy),
            brokerHost
        )
    );
    Assertions.assertEquals(
        String.join("\n", Stream.of(brokerHost, coordinatorHost).sorted().toList()),
        cluster.runSql(
            "SELECT DISTINCT server FROM sys.stack_trace WHERE server IN ('%s', '%s') ORDER BY server",
            nativeQueryContext(plannerStrategy),
            brokerHost,
            coordinatorHost
        )
    );
    Assertions.assertEquals(
        "",
        cluster.runSql(
            "SELECT DISTINCT server FROM sys.stack_trace WHERE server = 'unknown.host:1'",
            nativeQueryContext(plannerStrategy)
        )
    );
  }

  /**
   * A query on {@code sys.stack_trace} with a subquery on another native system table passes the planner's
   * authorization sanity check, which must account for system tables scanned inside subqueries. The {@code server}
   * filter is in the subquery on {@code sys.stack_trace}, so it applies directly to the stack-trace scan.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableWithSystemTableSubquery(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*) > 0
        FROM (SELECT server FROM sys.stack_trace WHERE server = '%s') t
        WHERE t.server IN (SELECT server FROM sys.server_properties WHERE property = 'druid.service')
        """,
        nativeQueryContext(plannerStrategy),
        host(broker)
    );

    Assertions.assertEquals("true", result.trim());
  }

  /**
   * Without {@code useDruidEngineForSystemTables}, a query with a subquery on {@code sys.server_properties} still
   * runs through the Broker's SQL interpreter.
   * <p>
   * This test lives with the {@code sys.stack_trace} tests because it guards a planner change made for that table:
   * the check that rejects {@code sys.stack_trace} without the Druid engine must not break the interpreter fallback
   * for other system tables.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_serverPropertiesSubqueryWithoutDruidEngine(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT server
        FROM sys.servers
        WHERE server IN (SELECT server FROM sys.server_properties WHERE property = 'druid.service')
        """,
        QueryContext.ofMap(
            QueryContextParameters.ENGINE,
            NativeSqlEngine.NAME,
            QueryContextParameters.PLANNER_STRATEGY,
            plannerStrategy
        )
    );

    Assertions.assertTrue(result.contains(host(broker)), result);
  }

  /**
   * The Broker rejects a SQL query with an invalid {@code maxStackTraceFrameDepth} before it queries any service.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableRejectsInvalidDepth(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final Map<String, Object> context = new HashMap<>(nativeQueryContext(plannerStrategy));
    context.putAll(invalidDepthContext());
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            """
            SELECT thread_name
            FROM sys.stack_trace
            WHERE server = '%s'
            """,
            context,
            host(broker)
        )
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(exception.getMessage().contains("maxStackTraceFrameDepth"), exception.getMessage());
  }

  /**
   * With the Druid engine, {@code sys.stack_trace} can't be combined with a system table that only the Broker's SQL
   * interpreter serves, which would send the whole query to the interpreter.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void test_stackTraceTableCannotBeCombinedWithInterpreterOnlyTable(
      final QueryContexts.PlannerStrategy plannerStrategy
  )
  {
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            """
            SELECT t.thread_name, s.server_type
            FROM sys.stack_trace t
            JOIN sys.servers s ON t.server = s.server
            WHERE t.server = '%s'
            """,
            nativeQueryContext(plannerStrategy),
            host(broker)
        )
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(
        exception.getMessage().contains(
            "sys.stack_trace can't be combined with system tables that aren't served by the Druid engine: "
            + "[sys.servers]"
        ),
        exception.getMessage()
    );
  }

  /**
   * A native query sent to a service reads only that service's threads, whether it has no {@code server} filter or a
   * filter that accepts other servers.
   */
  @Test
  public void test_nativeQueryReadsOnlyContactedService()
  {
    final String brokerHost = host(broker);
    final String coordinatorHost = host(coordinator);
    for (final DimFilter filter : Arrays.asList(
        null,
        new InDimFilter("server", List.of(brokerHost, coordinatorHost), null)
    )) {
      final StatusResponseHolder response = postNativeQuery(coordinator, stackTraceQuery(filter));

      Assertions.assertEquals(HttpResponseStatus.OK, response.getStatus(), response.getContent());
      Assertions.assertTrue(response.getContent().contains(coordinatorHost), response.getContent());
      Assertions.assertFalse(response.getContent().contains(brokerHost), response.getContent());
    }
  }

  /**
   * A native query with an invalid {@code maxStackTraceFrameDepth} is rejected by the contacted service.
   */
  @Test
  public void test_nativeQueryRejectsInvalidDepth()
  {
    for (final EmbeddedDruidServer<?> server : List.<EmbeddedDruidServer<?>>of(broker, coordinator)) {
      final StatusResponseHolder response = postNativeQuery(
          server,
          stackTraceQuery(null).withOverriddenContext(invalidDepthContext())
      );

      Assertions.assertEquals(HttpResponseStatus.BAD_REQUEST, response.getStatus(), response.getContent());
      Assertions.assertTrue(response.getContent().contains("maxStackTraceFrameDepth"), response.getContent());
    }
  }

  /**
   * The typed builders reject an out-of-range value, so the invalid context is built with the parameter name.
   */
  private static Map<String, Object> invalidDepthContext()
  {
    return Map.of(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.getName(), 5);
  }

  private static String host(final EmbeddedDruidServer<?> server)
  {
    return server.bindings().selfNode().getHostAndPortToUse();
  }

  private static ScanQuery stackTraceQuery(@Nullable final DimFilter filter)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource("stack_trace"))
                 .eternityInterval()
                 .columns("server", "thread_name")
                 .filters(filter)
                 .build();
  }

  private StatusResponseHolder postNativeQuery(final EmbeddedDruidServer<?> server, final ScanQuery query)
  {
    try {
      final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(server) + "/druid/v2/"))
          .setContent(MediaType.APPLICATION_JSON, server.bindings().jsonMapper().writeValueAsBytes(query));
      return server.bindings()
                   .globalHttpClient()
                   .go(request, StatusResponseHandler.getInstance())
                   .get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
