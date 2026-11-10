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

import java.util.List;
import java.util.Map;

/**
 * Cluster and helpers for the {@code sys.servers} authorization tests: a Broker with basic authentication and the
 * Druid engine enabled for system tables, queried with SQL and native queries as users with given permissions.
 */
public abstract class NativeSysServersAuthTestBase extends EmbeddedClusterTestBase
{
  protected static final ResourceAction READ_SERVERS_TABLE =
      new ResourceAction(new Resource(ServersTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
  protected static final ResourceAction READ_STATE =
      new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ);
  private static final String PASSWORD = "helloworld";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  protected HttpClient adminClient;

  /**
   * Returns the value of {@code druid.auth.authorizeSystemTablesDirectly} for the cluster.
   */
  protected abstract boolean authorizeSystemTablesDirectly();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addResource(new EmbeddedBasicAuthResource())
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .addCommonProperty(
                                   "druid.auth.authorizeSystemTablesDirectly",
                                   String.valueOf(authorizeSystemTablesDirectly())
                               )
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(new EmbeddedOverlord())
                               .addServer(broker);
  }

  @BeforeAll
  public void setupAdmin()
  {
    adminClient = client(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD);
  }

  /**
   * Returns the Broker's server name, as {@code sys.servers} reports it.
   */
  protected String brokerServer()
  {
    return broker.bindings().selfNode().getHostAndPortToUse();
  }

  /**
   * Sends SQL to the Broker with the Druid engine for system tables, and checks the response status.
   */
  protected StatusResponseHolder sql(
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

  /**
   * Checks, as the admin, that the Broker plans the SQL on the Druid engine, so the test can't pass on the Broker's
   * SQL interpreter, which returns the same rows and errors.
   */
  protected void assertPlannedOnDruidEngine(final String sql)
  {
    final String plan = sql(adminClient, "EXPLAIN PLAN FOR " + sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(plan.contains("systemTable"), plan);
  }

  /**
   * Sends a native Scan on {@code sys.servers} to the Broker, and checks the response status.
   */
  protected StatusResponseHolder nativeQuery(final HttpClient client, final HttpResponseStatus expectedStatus)
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

  /**
   * Creates a user with the given permissions, and returns a client authenticated as that user.
   */
  protected HttpClient createUser(final String user, final List<ResourceAction> permissions)
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
