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

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ServersTableDescriptor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Authorization of {@code sys.servers} on the Druid engine, with {@code druid.auth.authorizeSystemTablesDirectly}
 * set. A caller needs SYSTEM_TABLE READ on {@code servers} and STATE READ, whether it queries the table with SQL or
 * with a native query. The table is read on the Broker that runs the query, which applies the caller's access while
 * reading its own rows.
 */
public class NativeSysServersAuthTest extends EmbeddedClusterTestBase
{
  private static final String PASSWORD = "helloworld";
  private static final ResourceAction READ_SERVERS_TABLE =
      new ResourceAction(new Resource(ServersTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
  private static final ResourceAction READ_STATE =
      new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private HttpClient adminClient;
  private HttpClient sysTableWithStateUser;
  private HttpClient sysTableUser;
  private HttpClient stateUser;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addResource(new EmbeddedBasicAuthResource())
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .addCommonProperty("druid.auth.authorizeSystemTablesDirectly", "true")
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(new EmbeddedOverlord())
                               .addServer(broker);
  }

  @BeforeAll
  public void setupUsers()
  {
    adminClient = client(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD);
    sysTableWithStateUser = createUser("sysTableWithStateUser", List.of(READ_SERVERS_TABLE, READ_STATE));
    sysTableUser = createUser("sysTableUser", List.of(READ_SERVERS_TABLE));
    stateUser = createUser("stateUser", List.of(READ_STATE));
  }

  /**
   * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, and STATE READ.
   */
  @Test
  public void testSqlRequiresSystemTableAndStateRead()
  {
    final String sql = "SELECT server, server_type FROM sys.servers WHERE server_type = 'broker'";

    final String plan = sql(adminClient, "EXPLAIN PLAN FOR " + sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(plan.contains("systemTable"), plan);

    final String rows = sql(sysTableWithStateUser, sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(rows.contains(broker.bindings().selfNode().getHostAndPortToUse() + ",broker"), rows);

    final String forbidden = sql(sysTableUser, sql, HttpResponseStatus.FORBIDDEN).getContent();
    Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);

    final String invisible = sql(stateUser, sql, HttpResponseStatus.BAD_REQUEST).getContent();
    Assertions.assertTrue(invisible.contains("Object 'servers' not found within 'sys'"), invisible);
  }

  /**
   * A native query on the Broker needs the same two permissions.
   */
  @Test
  public void testNativeQueryRequiresSystemTableAndStateRead()
  {
    final StatusResponseHolder rows = nativeQuery(sysTableWithStateUser, HttpResponseStatus.OK);
    Assertions.assertTrue(
        rows.getContent().contains(broker.bindings().selfNode().getHostAndPortToUse()),
        rows.getContent()
    );

    final StatusResponseHolder forbidden = nativeQuery(sysTableUser, HttpResponseStatus.FORBIDDEN);
    Assertions.assertTrue(
        forbidden.getContent().contains("Insufficient permission to view servers"),
        forbidden.getContent()
    );

    final StatusResponseHolder invisible = nativeQuery(stateUser, HttpResponseStatus.FORBIDDEN);
    Assertions.assertTrue(invisible.getContent().contains("Unauthorized"), invisible.getContent());
  }

  private StatusResponseHolder sql(
      final HttpClient client,
      final String sql,
      final HttpResponseStatus expectedStatus
  )
  {
    final Map<String, Object> request = Map.of(
        "query", sql,
        "context", QueryContext.ofMap(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES, true),
        "resultFormat", "csv"
    );
    return HttpUtil.makeRequest(
        client,
        HttpMethod.POST,
        getServerUrl(broker) + "/druid/v2/sql",
        request,
        expectedStatus
    );
  }

  private StatusResponseHolder nativeQuery(final HttpClient client, final HttpResponseStatus expectedStatus)
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(ServersTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .columns("server", "server_type")
                                  .build();
    return HttpUtil.makeRequest(
        client,
        HttpMethod.POST,
        getServerUrl(broker) + "/druid/v2/",
        query,
        expectedStatus
    );
  }

  private HttpClient createUser(final String user, final List<ResourceAction> permissions)
  {
    EmbeddedBasicAuthResource.createUserWithPermissions(
        adminClient,
        coordinator,
        user,
        PASSWORD,
        user + "Role",
        permissions
    );
    return client(user, PASSWORD);
  }

  private HttpClient client(final String user, final String password)
  {
    return new CredentialedHttpClient(new BasicCredentials(user, password), broker.bindings().globalHttpClient());
  }
}
