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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryResource;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DartSysServerPropertiesQueryTest extends EmbeddedClusterTestBase
{
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final String PROPERTY_PREFIX = "dart.e2e.server.properties.";
  private static final String COORDINATOR_SERVICE = "dart/e2e/coordinator";
  private static final String OVERLORD_SERVICE = "dart/e2e/overlord";
  private static final String BROKER_SERVICE = "dart/e2e/broker";
  private static final String HISTORICAL_SERVICE = "dart/e2e/historical";
  private static final String INDEXER_SERVICE = "dart/e2e/indexer";
  private static final String ROUTER_SERVICE = "dart/e2e/router";

  private final EmbeddedCoordinator coordinator = server(
      new EmbeddedCoordinator(),
      COORDINATOR_SERVICE,
      "coordinator"
  );
  private final EmbeddedOverlord overlord = server(new EmbeddedOverlord(), OVERLORD_SERVICE, "overlord");
  private final EmbeddedBroker broker = server(
      new EmbeddedBroker()
          .setServerMemory(1_000_000_000L)
          .setServerDirectMemory(1_000_000_000L)
          .addProperty("druid.msq.dart.controller.heapFraction", "0.5"),
      BROKER_SERVICE,
      "broker"
  );
  private final EmbeddedHistorical historical = server(
      new EmbeddedHistorical()
          .setServerMemory(1_000_000_000L)
          .setServerDirectMemory(1_000_000_000L)
          .addProperty("druid.msq.dart.worker.heapFraction", "0.5"),
      HISTORICAL_SERVICE,
      "historical"
  );
  private final EmbeddedIndexer indexer = server(new EmbeddedIndexer(), INDEXER_SERVICE, "indexer");
  private final EmbeddedRouter router = server(new EmbeddedRouter(), ROUTER_SERVICE, "router");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(indexer)
                               .addServer(router);
  }

  /**
   * {@code SELECT service_name, "value" FROM sys.server_properties WHERE property LIKE 'dart.e2e.%'
   * ORDER BY service_name}
   *
   * <p>Every persistent node type contributes one row. The Historical reads its co-located provider, while the
   * embedded Broker worker proxies the control-plane nodes.</p>
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testFansOutToHistoricalAndBrokerProxiedNodes(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT service_name, \"value\" "
        + "FROM sys.server_properties "
        + "WHERE property LIKE '" + PROPERTY_PREFIX + "%%' "
        + "ORDER BY service_name",
        dartQueryContext(plannerStrategy)
    );

    Assertions.assertEquals(
        String.join(
            "\n",
            BROKER_SERVICE + ",broker",
            COORDINATOR_SERVICE + ",coordinator",
            HISTORICAL_SERVICE + ",historical",
            INDEXER_SERVICE + ",indexer",
            OVERLORD_SERVICE + ",overlord",
            ROUTER_SERVICE + ",router"
        ),
        result
    );
  }

  /**
   * {@code SELECT COUNT(*), COUNT(DISTINCT service_name), COUNT(DISTINCT server), COUNT(DISTINCT property), SUM(1)
   * FROM sys.server_properties WHERE property LIKE 'dart.e2e.%'}
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testDistributedAggregations(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT service_name), COUNT(DISTINCT server), "
        + "COUNT(DISTINCT property), SUM(1) "
        + "FROM sys.server_properties "
        + "WHERE property LIKE '" + PROPERTY_PREFIX + "%%'",
        dartQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("6,6,6,6,6", result);
  }

  /**
   * {@code SELECT service_name, "value" FROM sys.server_properties WHERE property IN (...)
   * ORDER BY service_name DESC LIMIT 2}
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testFilterProjectionGlobalOrderAndLimit(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT service_name, \"value\" "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + property("broker") + "', '" + property("historical") + "', '"
        + property("router") + "') "
        + "ORDER BY service_name DESC LIMIT 2",
        dartQueryContext(plannerStrategy)
    );

    Assertions.assertEquals(
        String.join("\n", ROUTER_SERVICE + ",router", HISTORICAL_SERVICE + ",historical"),
        result
    );
  }

  /**
   * Scans, grouped and nested aggregates, self-joins, windows, and ordered LIMIT/OFFSET queries agree with
   * the default two-stage pipeline and an explicit single-stage setting in both planner modes.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testStageConcurrencyAcrossWorkloads(final String plannerStrategy)
  {
    final Map<String, Object> defaultContext = new HashMap<>(dartQueryContext(plannerStrategy));
    defaultContext.put("useApproximateCountDistinct", false);
    final Map<String, Object> singleStageContext = new HashMap<>(defaultContext);
    singleStageContext.put("maxConcurrentStages", 1);
    final String filteredTable = "SELECT * FROM sys.server_properties WHERE property LIKE '"
                                 + PROPERTY_PREFIX + "%%'";
    // Plain ordered scans and COUNT(DISTINCT) are covered with absolute expectations by the tests above.
    final List<String> queries = new ArrayList<>(List.of(
        "SELECT \"value\", COUNT(*), COUNT(DISTINCT server) FROM (" + filteredTable + ") "
        + "GROUP BY \"value\" HAVING COUNT(*) > 0 ORDER BY \"value\"",
        "SELECT SUM(n), COUNT(*) FROM (SELECT service_name, COUNT(*) AS n FROM (" + filteredTable
        + ") GROUP BY service_name)",
        "SELECT a.service_name, COUNT(*) FROM (" + filteredTable + ") a "
        + "JOIN (" + filteredTable + ") b ON a.server = b.server GROUP BY a.service_name ORDER BY a.service_name",
        "SELECT service_name FROM (" + filteredTable + ") ORDER BY service_name DESC LIMIT 2 OFFSET 1"
    ));
    // Window planning is currently available only in coupled mode.
    if (QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED.equals(plannerStrategy)) {
      queries.add(
          "SELECT service_name, ROW_NUMBER() OVER (PARTITION BY node_roles ORDER BY service_name) "
          + "FROM (" + filteredTable + ") ORDER BY service_name"
      );
    }

    for (final String sql : queries) {
      final String singleStageResult = cluster.runSql(sql, singleStageContext);
      Assertions.assertFalse(singleStageResult.isEmpty(), sql);
      Assertions.assertEquals(singleStageResult, cluster.runSql(sql, defaultContext), sql);
    }
  }

  /** UNION ALL over system tables is rejected at planning time instead of failing during execution. */
  @Test
  public void testUnionAllIsRejectedAtPlanning()
  {
    final RuntimeException failure = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql(
            "SELECT service_name FROM sys.server_properties UNION ALL SELECT service_name FROM sys.server_properties",
            dartQueryContext(QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED)
        )
    );
    Assertions.assertTrue(
        failure.getMessage().contains("Union operation is only supported between regular tables"),
        failure.getMessage()
    );
  }

  /** Every node role serves an internal {@code ScanQuery(SystemTableDataSource)} through the standard endpoint. */
  @Test
  public void testNodeLocalNativeScanEndpointOnEveryNode() throws Exception
  {
    assertNodeLocalScan(coordinator, property("coordinator"), "coordinator");
    assertNodeLocalScan(overlord, property("overlord"), "overlord");
    assertNodeLocalScan(broker, property("broker"), "broker");
    assertNodeLocalScan(historical, property("historical"), "historical");
    assertNodeLocalScan(indexer, property("indexer"), "indexer");
    assertNodeLocalScan(router, property("router"), "router");

    final HttpResponse<String> invalidResponse = sendNativeQuery(
        coordinator,
        """
        {
          "queryType": "scan",
          "dataSource": {"type": "table", "name": "missing"},
          "intervals": ["1900/3000"],
          "resultFormat": "compactedList",
          "columns": []
        }
        """
    );
    Assertions.assertEquals(400, invalidResponse.statusCode());

    final HttpResponse<String> nonLocalSystemTableResponse = sendNativeQuery(
        broker,
        systemTableScanQuery(property("broker")),
        false
    );
    Assertions.assertEquals(400, nonLocalSystemTableResponse.statusCode());
  }

  private void assertNodeLocalScan(
      final EmbeddedDruidServer<?> server,
      final String propertyName,
      final String expectedValue
  ) throws Exception
  {
    final HttpResponse<String> response = sendNativeQuery(
        server,
        systemTableScanQuery(propertyName)
    );
    Assertions.assertEquals(200, response.statusCode(), response.body());
    Assertions.assertTrue(response.body().contains(propertyName), response.body());
    Assertions.assertTrue(response.body().contains(expectedValue), response.body());
  }

  private HttpResponse<String> sendNativeQuery(
      final EmbeddedDruidServer<?> server,
      final String query
  ) throws Exception
  {
    return sendNativeQuery(server, query, true);
  }

  private HttpResponse<String> sendNativeQuery(
      final EmbeddedDruidServer<?> server,
      final String query,
      final boolean routeLocally
  ) throws Exception
  {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerUrl(server) + "/druid/v2/"))
                                                   .header("Content-Type", "application/json")
                                                   .timeout(Duration.ofSeconds(10))
                                                   .POST(HttpRequest.BodyPublishers.ofString(query));
    if (routeLocally) {
      builder.header(QueryResource.HEADER_NATIVE_QUERY_ROUTE, QueryResource.NATIVE_QUERY_ROUTE_LOCAL);
    }
    final HttpRequest request = builder.build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static String systemTableScanQuery(final String propertyName)
  {
    return StringUtils.format(
        """
        {
          "queryType": "scan",
          "dataSource": {"type": "systemTable", "table": "server_properties"},
          "intervals": ["1900/3000"],
          "resultFormat": "compactedList",
          "columns": ["property", "value"],
          "filter": {"type": "selector", "dimension": "property", "value": "%s"}
        }
        """,
        propertyName
    );
  }

  private static Map<String, Object> dartQueryContext(final String plannerStrategy)
  {
    return Map.of(
        QueryContexts.ENGINE,
        DartSqlEngine.NAME,
        QueryContexts.CTX_NATIVE_QUERY_SQL_PLANNING_MODE,
        plannerStrategy
    );
  }

  private static String property(final String nodeType)
  {
    return PROPERTY_PREFIX + nodeType;
  }

  private static <T extends EmbeddedDruidServer<T>> T server(
      final T server,
      final String service,
      final String nodeType
  )
  {
    return server.addProperty("druid.service", service).addProperty(property(nodeType), nodeType);
  }
}
