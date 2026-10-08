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

import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

public class NativeSysConfigurationQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.indexer.tasklock.batchAllocationNumThreads", "0");
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addCommonProperty("druid.centralizedDatasourceSchema.enabled", "true")
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(indexer)
                               .addServer(router);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testConfigurationFansOutToAllNodes(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT COUNT(DISTINCT server), COUNT(DISTINCT service_name) FROM sys.configuration "
        + "WHERE property = 'druid.service' AND value_status = 'AVAILABLE'",
        nativeQueryContext(plannerStrategy)
    );
    Assertions.assertEquals("6,6", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testTaskLockDefaultsAndExplicitValues(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT property, effective_value, value_status FROM sys.configuration "
        + "WHERE service_name = 'druid/overlord' AND property IN "
        + "('druid.indexer.tasklock.batchAllocationNumThreads', 'druid.indexer.tasklock.forceTimeChunkLock') "
        + "ORDER BY property",
        nativeQueryContext(plannerStrategy)
    );
    Assertions.assertEquals(
        "druid.indexer.tasklock.batchAllocationNumThreads,1,AVAILABLE\n"
        + "druid.indexer.tasklock.forceTimeChunkLock,true,AVAILABLE",
        result
    );
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
