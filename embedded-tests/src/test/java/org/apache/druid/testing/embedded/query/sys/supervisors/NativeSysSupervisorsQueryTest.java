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

package org.apache.druid.testing.embedded.query.sys.supervisors;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.indexing.overlord.supervisor.NoopSupervisorSpec;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.system.table.SupervisorTableDescriptor;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.ws.rs.core.MediaType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NativeSysSupervisorsQueryTest extends EmbeddedClusterTestBase
{
  private static final String SUPERVISOR_PREFIX = "native_sys_supervisor_";
  private static final String ALL_COLUMNS_SQL = """
      SELECT supervisor_id, datasource, state, detailed_state, healthy, type, source, suspended, spec
      FROM sys.supervisors
      WHERE supervisor_id IN ('native_sys_supervisor_a', 'native_sys_supervisor_b')
      """;

  /**
   * Two Overlords, so that only one of them is the leader that serves {@code sys.supervisors}.
   */
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedOverlord secondOverlord = new EmbeddedOverlord().addProperty("druid.plaintextPort", "7090");
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .addServer(new EmbeddedCoordinator())
                               .addServer(overlord)
                               .addServer(secondOverlord)
                               .addServer(broker);
  }

  @BeforeAll
  public void createSupervisors() throws InterruptedException
  {
    waitForOverlordLeader();
    cluster.callApi().postSupervisor(
        new NoopSupervisorSpec(SUPERVISOR_PREFIX + "a", List.of("native_sys_supervisor_datasource_a"))
    );
    cluster.callApi().postSupervisor(
        new NoopSupervisorSpec(SUPERVISOR_PREFIX + "b", List.of("native_sys_supervisor_datasource_b"))
    );
  }

  @Test
  public void testFilteredGroupByUsesOverlordProvider()
  {
    final String result = cluster.runSql(
        """
        SELECT datasource, COUNT(*), SUM(healthy)
        FROM sys.supervisors
        WHERE supervisor_id IN ('native_sys_supervisor_a', 'native_sys_supervisor_b')
        GROUP BY datasource
        ORDER BY datasource
        """,
        nativeQueryContext()
    );

    Assertions.assertEquals(
        """
        native_sys_supervisor_datasource_a,1,1
        native_sys_supervisor_datasource_b,1,1""",
        result
    );
  }

  /**
   * Native execution returns the same rows as the Broker's SQL interpreter.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeResultsMatchSqlInterpreter(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final Map<String, Object> context = nativeQueryContext(plannerStrategy);
    if (plannerStrategy == QueryContexts.PlannerStrategy.COUPLED) {
      // Only the coupled planner explains the native query, which names the system table it reads.
      final String plan = cluster.runSql("EXPLAIN PLAN FOR " + ALL_COLUMNS_SQL, context);
      Assertions.assertTrue(plan.contains("systemTable"), plan);
    }

    final List<String> nativeRows =
        Arrays.stream(cluster.runSql(ALL_COLUMNS_SQL, context).split("\n")).sorted().toList();
    final List<String> sqlRows = Arrays.stream(
        cluster.runSql(
            ALL_COLUMNS_SQL,
            QueryContext.ofMap(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES, false)
        ).split("\n")
    ).sorted().toList();
    Assertions.assertEquals(2, nativeRows.size(), String.join("\n", nativeRows));
    Assertions.assertEquals(sqlRows, nativeRows);
  }

  /**
   * SQL reads sys.supervisors from the leader Overlord only. Reading the standby as well would fail the query, since
   * the standby rejects the read, or would repeat every supervisor.
   */
  @Test
  public void testSqlReadsSupervisorsFromLeaderOverlordOnly()
  {
    Assertions.assertEquals(
        "2",
        // The SQL is a format string, so pass the LIKE pattern as an argument to keep its wildcard intact.
        cluster.runSql(
            "SELECT COUNT(*) FROM sys.supervisors WHERE supervisor_id LIKE '%s'",
            nativeQueryContext(),
            SUPERVISOR_PREFIX + "%"
        )
    );
  }

  /**
   * A native query sent to an Overlord reads only that Overlord's rows, so the standby rejects it rather than return
   * a table it doesn't own, and the leader serves it.
   */
  @Test
  public void testNativeQueryIsServedByLeaderOverlordOnly() throws Exception
  {
    Assertions.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, postSupervisorsScan(standby()).getStatus());

    final StatusResponseHolder leaderResponse = postSupervisorsScan(leader());
    Assertions.assertEquals(HttpResponseStatus.OK, leaderResponse.getStatus(), leaderResponse.getContent());
    Assertions.assertTrue(
        leaderResponse.getContent().contains(SUPERVISOR_PREFIX + "a"),
        leaderResponse.getContent()
    );
  }

  private StatusResponseHolder postSupervisorsScan(final EmbeddedOverlord target) throws Exception
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(SupervisorTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .columns("supervisor_id")
                                  .build();
    final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(target) + "/druid/v2/"))
        .setContent(MediaType.APPLICATION_JSON, target.bindings().jsonMapper().writeValueAsBytes(query));
    return target.bindings().globalHttpClient().go(request, StatusResponseHandler.getInstance()).get();
  }

  /**
   * Waits until one of the Overlords is the leader, since an election may still be running when the cluster starts.
   */
  private void waitForOverlordLeader() throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!isLeader(overlord) && !isLeader(secondOverlord)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("No Overlord became the leader");
      }
      Thread.sleep(100);
    }
  }

  private static boolean isLeader(final EmbeddedOverlord candidate)
  {
    return candidate.bindings().overlordLeaderSelector().isLeader();
  }

  private EmbeddedOverlord leader()
  {
    return isLeader(overlord) ? overlord : secondOverlord;
  }

  private EmbeddedOverlord standby()
  {
    return leader() == overlord ? secondOverlord : overlord;
  }

  private static Map<String, Object> nativeQueryContext()
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true
    );
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
