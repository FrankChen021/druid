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

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.indexing.overlord.supervisor.NoopSupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.DruidMetrics;
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

import java.util.List;
import java.util.Map;

public class NativeSysSupervisorsQueryTest extends EmbeddedClusterTestBase
{
  private static final String SUPERVISOR_PREFIX = "native_sys_supervisor_";

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(new EmbeddedCoordinator())
                               .addServer(overlord)
                               .addServer(broker);
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

  /**
   * A filtered GroupBy reads its rows through the native {@code systemTable} datasource, served by the Overlord. The
   * SQL interpreter would return the same rows, so the Overlord's query metric is checked to show the native path ran.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testFilteredGroupByUsesNativeSystemTable(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String sql = StringUtils.format(
        """
        SELECT datasource, COUNT(*), SUM(healthy)
        FROM sys.supervisors
        WHERE supervisor_id IN ('%sa', '%sb')
        GROUP BY datasource
        ORDER BY datasource
        """,
        SUPERVISOR_PREFIX,
        SUPERVISOR_PREFIX
    );

    overlord.latchableEmitter().flush();
    Assertions.assertEquals(
        """
        native_sys_supervisor_datasource_a,1,1
        native_sys_supervisor_datasource_b,1,1""",
        cluster.runSql(sql, nativeQueryContext(plannerStrategy))
    );
    // The Overlord serves the native system-table Scan through its query endpoint, which the SQL interpreter's REST
    // call to the supervisor API does not.
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("query/time")
                      .hasDimension(DruidMetrics.DATASOURCE, "sys." + SupervisorTableDescriptor.TABLE_NAME)
    );
  }

  /** A native Scan of every column returns the public table, without the column the Broker uses for authorization. */
  @Test
  public void testNativeScanOmitsAuthorizationColumn() throws Exception
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(SupervisorTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .build();
    final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(broker) + "/druid/v2/"))
        .setContent(MediaType.APPLICATION_JSON, broker.bindings().jsonMapper().writeValueAsBytes(query));
    final StatusResponseHolder response = broker.bindings()
                                                .globalHttpClient()
                                                .go(request, StatusResponseHandler.getInstance())
                                                .get();

    Assertions.assertEquals(HttpResponseStatus.OK, response.getStatus(), response.getContent());
    Assertions.assertTrue(response.getContent().contains(SUPERVISOR_PREFIX + "a"), response.getContent());
    Assertions.assertTrue(response.getContent().contains("\"supervisor_id\""), response.getContent());
    Assertions.assertFalse(
        response.getContent().contains(SupervisorTableDescriptor.AUTHORIZATION_DATASOURCES_COLUMN),
        response.getContent()
    );
  }

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_QUERY_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
