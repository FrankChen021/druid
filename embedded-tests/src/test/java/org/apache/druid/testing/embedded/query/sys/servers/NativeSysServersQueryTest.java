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

package org.apache.druid.testing.embedded.query.sys.servers;

import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

public class NativeSysServersQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.labels", "{\"environment\":\"test\"}");
  /**
   * A second Broker, whose view of the cluster would repeat every row if SQL gathered {@code sys.servers} from every
   * Broker.
   */
  private final EmbeddedBroker secondBroker = new EmbeddedBroker()
      .addProperty("druid.plaintextPort", "7093");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .useLatchableEmitter()
                               .addServer(new EmbeddedCoordinator())
                               .addServer(new EmbeddedOverlord())
                               .addServer(broker)
                               .addServer(secondBroker);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testJsonValueOnJsonLabels(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT JSON_VALUE(labels_json, '$.environment')
        FROM sys.servers
        WHERE server = '%s'
        """,
        nativeQueryContext(plannerStrategy),
        broker.bindings().selfNode().getHostAndPortToUse()
    );

    Assertions.assertEquals("test", result);
  }

  /**
   * {@code sys.servers} is read only on the querying Broker, so each server appears once, even with two Brokers.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testEachServerAppearsOnce(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT server_type, COUNT(*), COUNT(DISTINCT server)
        FROM sys.servers
        GROUP BY server_type
        ORDER BY server_type
        """,
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals(
        """
        broker,2,2
        coordinator,1,1
        overlord,1,1""",
        result
    );
  }

  /**
   * A join reads the local-scoped table on both sides, and matches each server with itself only.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testSelfJoin(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*)
        FROM sys.servers s1
        INNER JOIN sys.servers s2 ON s1.server = s2.server
        """,
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("4", result);
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
