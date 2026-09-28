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

import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
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

import java.util.Map;

public class DartSysServerPropertiesQueryTest extends EmbeddedClusterTestBase
{
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
  @Test
  public void testFansOutToHistoricalAndBrokerProxiedNodes()
  {
    final String result = cluster.runSql(
        "SELECT service_name, \"value\" "
        + "FROM sys.server_properties "
        + "WHERE property LIKE '" + PROPERTY_PREFIX + "%%' "
        + "ORDER BY service_name",
        dartQueryContext()
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
  @Test
  public void testDistributedAggregations()
  {
    final String result = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT service_name), COUNT(DISTINCT server), "
        + "COUNT(DISTINCT property), SUM(1) "
        + "FROM sys.server_properties "
        + "WHERE property LIKE '" + PROPERTY_PREFIX + "%%'",
        dartQueryContext()
    );

    Assertions.assertEquals("6,6,6,6,6", result);
  }

  /**
   * {@code SELECT service_name, "value" FROM sys.server_properties WHERE property IN (...)
   * ORDER BY service_name DESC LIMIT 2}
   */
  @Test
  public void testFilterProjectionGlobalOrderAndLimit()
  {
    final String result = cluster.runSql(
        "SELECT service_name, \"value\" "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + property("broker") + "', '" + property("historical") + "', '"
        + property("router") + "') "
        + "ORDER BY service_name DESC LIMIT 2",
        dartQueryContext()
    );

    Assertions.assertEquals(
        String.join("\n", ROUTER_SERVICE + ",router", HISTORICAL_SERVICE + ",historical"),
        result
    );
  }

  private static Map<String, Object> dartQueryContext()
  {
    return Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME);
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
