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

import org.apache.druid.indexing.overlord.supervisor.NoopSupervisorSpec;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

public class NativeSysSupervisorsQueryTest extends EmbeddedClusterTestBase
{
  private static final String SUPERVISOR_PREFIX = "native_sys_supervisor_";

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(new EmbeddedCoordinator())
                               .addServer(new EmbeddedOverlord())
                               .addServer(new EmbeddedBroker());
  }

  @BeforeAll
  public void createSupervisors()
  {
    cluster.callApi().postSupervisor(
        new NoopSupervisorSpec(SUPERVISOR_PREFIX + "a", List.of("native_sys_supervisor_datasource_a"))
    );
    cluster.callApi().postSupervisor(
        new NoopSupervisorSpec(SUPERVISOR_PREFIX + "b", List.of("native_sys_supervisor_datasource_b"))
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testFilteredGroupByUsesOverlordProvider(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT datasource, COUNT(*) "
        + "FROM sys.supervisors "
        + "WHERE supervisor_id = '" + SUPERVISOR_PREFIX + "a' "
        + "GROUP BY datasource",
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("native_sys_supervisor_datasource_a,1", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testNativeAggregationsSupportDistinctCount(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT supervisor_id), COUNT(DISTINCT datasource), SUM(healthy) "
        + "FROM sys.supervisors "
        + "WHERE supervisor_id IN ('" + SUPERVISOR_PREFIX + "a', '" + SUPERVISOR_PREFIX + "b')",
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("2,2,2,2", result);
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
