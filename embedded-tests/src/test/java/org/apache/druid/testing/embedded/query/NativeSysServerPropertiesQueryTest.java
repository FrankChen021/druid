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

import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class NativeSysServerPropertiesQueryTest extends EmbeddedClusterTestBase
{
  private static final String SERVICE_NAME = "native/mvp/broker";
  private static final String COORDINATOR_PROPERTY = "native.sys.server.properties.coordinator";
  private static final String OVERLORD_PROPERTY = "native.sys.server.properties.overlord";
  private static final String BROKER_PROPERTY = "native.sys.server.properties.broker";
  private static final String HISTORICAL_PROPERTY = "native.sys.server.properties.historical";
  private static final String INDEXER_PROPERTY = "native.sys.server.properties.indexer";
  private static final String ROUTER_PROPERTY = "native.sys.server.properties.router";

  private static final Map<String, Object> NATIVE_QUERY_CONTEXT =
      Map.of(PlannerConfig.CTX_ENABLE_NATIVE_QUERY_FOR_SYSTEM_TABLES, true);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty(COORDINATOR_PROPERTY, "enabled");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty(OVERLORD_PROPERTY, "enabled");

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", SERVICE_NAME)
      .addProperty(BROKER_PROPERTY, "enabled");

  private final EmbeddedHistorical historical = new EmbeddedHistorical()
      .addProperty(HISTORICAL_PROPERTY, "enabled");

  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .addProperty(INDEXER_PROPERTY, "enabled");

  private final EmbeddedRouter router = new EmbeddedRouter()
      .addProperty(ROUTER_PROPERTY, "enabled");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(indexer)
                               .addServer(router);
  }

  @Test
  public void testServerPropertiesFansOutToAllComponents()
  {
    final String result = cluster.runSql(
        "SELECT service_name, COUNT(*) "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + COORDINATOR_PROPERTY + "', '" + OVERLORD_PROPERTY + "', '" + BROKER_PROPERTY
        + "', '" + HISTORICAL_PROPERTY + "', '" + INDEXER_PROPERTY + "', '" + ROUTER_PROPERTY + "') "
        + "GROUP BY service_name ORDER BY service_name",
        NATIVE_QUERY_CONTEXT
    );

    Assertions.assertEquals(
        String.join(
            "\n",
            "druid/coordinator,1",
            "druid/historical,1",
            "druid/indexer,1",
            "druid/overlord,1",
            "druid/router,1",
            SERVICE_NAME + ",1"
        ),
        result
    );
  }
}
