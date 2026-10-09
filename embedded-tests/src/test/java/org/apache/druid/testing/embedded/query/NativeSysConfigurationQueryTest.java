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
import java.util.Set;

public class NativeSysConfigurationQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.indexer.tasklock.batchAllocationNumThreads", "0");
  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.server.hiddenProperties", "[\"password\",\"secret\",\"token\",\"key\"]")
      .addProperty("druid.server.maxSize", "2MiB");
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

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testJsonCollectionsAndReadableBytes(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT JSON_VALUE(PARSE_JSON(effective_value), '$[0]'), "
        + "JSON_VALUE(PARSE_JSON(effective_value), '$[1]'), "
        + "JSON_VALUE(PARSE_JSON(effective_value), '$[2]'), "
        + "JSON_VALUE(PARSE_JSON(effective_value), '$[3]') FROM sys.configuration "
        + "WHERE service_name = 'druid/broker' AND property = 'druid.server.hiddenProperties'",
        nativeQueryContext(plannerStrategy)
    );
    // A Set has no prescribed iteration order. All members must survive native transport and JSON extraction.
    Assertions.assertEquals(Set.of("password", "secret", "token", "key"), Set.of(result.split(",")));
    Assertions.assertEquals(
        "2.00 MiB",
        cluster.runSql(
            "SELECT effective_value FROM sys.configuration "
            + "WHERE service_name = 'druid/broker' AND property = 'druid.server.maxSize'",
            nativeQueryContext(plannerStrategy)
        )
    );
    Assertions.assertEquals(
        "PT10M",
        cluster.runSql(
            "SELECT effective_value FROM sys.configuration "
            + "WHERE service_name = 'druid/coordinator' AND property = 'druid.manager.rules.alertThreshold'",
            nativeQueryContext(plannerStrategy)
        )
    );
    Assertions.assertEquals(
        "VARCHAR",
        cluster.runSql(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sys' "
            + "AND TABLE_NAME = 'configuration' AND COLUMN_NAME = 'effective_value'",
            nativeQueryContext(plannerStrategy)
        )
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testHttpPropertiesAppearOnceAndBindingColumnIsAbsent(final String plannerStrategy)
  {
    final String[] counts = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT property) FROM sys.configuration "
        + "WHERE service_name = 'druid/broker' AND property LIKE 'druid.broker.http.%%'",
        nativeQueryContext(plannerStrategy)
    ).split(",");
    Assertions.assertTrue(Long.parseLong(counts[0]) > 0);
    Assertions.assertEquals(counts[0], counts[1]);
    Assertions.assertEquals(
        "",
        cluster.runSql(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sys' "
            + "AND TABLE_NAME = 'configuration' AND COLUMN_NAME = 'binding'",
            nativeQueryContext(plannerStrategy)
        )
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
