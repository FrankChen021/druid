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
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DartSysServerPropertiesNoHistoricalQueryTest extends EmbeddedClusterTestBase
{
  private static final String BROKER_PROPERTY = "dart.e2e.noHistorical.broker";
  private static final String COORDINATOR_PROPERTY = "dart.e2e.noHistorical.coordinator";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .setServerMemory(1_000_000_000L)
      .setServerDirectMemory(1_000_000_000L)
      .addProperty("druid.msq.dart.controller.heapFraction", "0.5")
      .addProperty(BROKER_PROPERTY, "broker");
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty(COORDINATOR_PROPERTY, "coordinator");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .addServer(coordinator)
                               .addServer(broker);
  }

  /**
   * {@code SELECT COUNT(*), COUNT(DISTINCT service_name) FROM sys.server_properties WHERE property IN (...)}
   *
   * <p>A Dart system-table query remains executable through the embedded Broker worker when the cluster has no
   * Historical workers.</p>
   */
  @Test
  public void testBrokerWorkerExecutesWithoutHistorical()
  {
    final String result = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT service_name) "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + BROKER_PROPERTY + "', '" + COORDINATOR_PROPERTY + "')",
        Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME)
    );

    Assertions.assertEquals("2,2", result);
  }
}
