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

package org.apache.druid.testing.embedded.query.sys.stack_trace;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.common.StringUtils;
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
import org.apache.druid.server.system.table.StackTraceTableDescriptor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Authorization of {@code sys.stack_trace} on the Druid engine, with SQL and native queries, as users with given
 * permissions. Each nested class runs its own cluster, with or without
 * {@code druid.auth.authorizeSystemTablesDirectly}.
 */
public class NativeSysStackTraceAuthTest
{
  /**
   * Authorization with {@code druid.auth.authorizeSystemTablesDirectly} set. A caller needs SYSTEM_TABLE READ on
   * {@code stack_trace} and STATE READ, whether it queries the table with SQL or with a native query.
   */
  @Nested
  public class AuthorizeSystemTablesDirectly extends AuthTestBase
  {
    private HttpClient sysTableWithStateUser;
    private HttpClient sysTableUser;
    private HttpClient stateUser;

    @Override
    protected boolean authorizeSystemTablesDirectly()
    {
      return true;
    }

    @BeforeAll
    public void setupUsers()
    {
      sysTableWithStateUser = createUser("sysTableWithStateUser", List.of(READ_STACK_TRACE_TABLE, READ_STATE));
      sysTableUser = createUser("sysTableUser", List.of(READ_STACK_TRACE_TABLE));
      stateUser = createUser("stateUser", List.of(READ_STATE));
    }

    /**
     * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, and STATE READ.
     */
    @Test
    public void testSqlRequiresSystemTableAndStateRead()
    {
      final String sql = brokerStackTraceSql();

      assertPlannedOnDruidEngine(sql);

      final String rows = sql(sysTableWithStateUser, sql, HttpResponseStatus.OK).getContent();
      Assertions.assertTrue(rows.contains(brokerHost()), rows);

      final String forbidden = sql(sysTableUser, sql, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains(INSUFFICIENT_PERMISSION), forbidden);

      final String invisible = sql(stateUser, sql, HttpResponseStatus.BAD_REQUEST).getContent();
      Assertions.assertTrue(invisible.contains("Object 'stack_trace' not found within 'sys'"), invisible);
    }

    /**
     * A native query needs the same two permissions, and no {@code server} filter, on the Broker and on any other
     * service.
     */
    @Test
    public void testNativeQueryRequiresSystemTableAndStateRead()
    {
      for (final EmbeddedDruidServer<?> server : servers()) {
        final String host = server.bindings().selfNode().getHostAndPortToUse();
        final StatusResponseHolder rows = nativeQuery(server, sysTableWithStateUser, HttpResponseStatus.OK);
        Assertions.assertTrue(rows.getContent().contains(host), rows.getContent());

        final StatusResponseHolder forbidden = nativeQuery(server, sysTableUser, HttpResponseStatus.FORBIDDEN);
        Assertions.assertTrue(forbidden.getContent().contains(INSUFFICIENT_PERMISSION), forbidden.getContent());

        final StatusResponseHolder invisible = nativeQuery(server, stateUser, HttpResponseStatus.FORBIDDEN);
        Assertions.assertTrue(invisible.getContent().contains("Unauthorized"), invisible.getContent());
        Assertions.assertFalse(invisible.getContent().contains(INSUFFICIENT_PERMISSION), invisible.getContent());
      }
    }
  }

  /**
   * Authorization without {@code druid.auth.authorizeSystemTablesDirectly}, the default. System tables then have no
   * table-level check, so STATE READ alone lets a caller read the table, with SQL or with a native query.
   */
  @Nested
  public class DefaultAuthorization extends AuthTestBase
  {
    private HttpClient stateUser;
    private HttpClient noPermissionUser;

    @Override
    protected boolean authorizeSystemTablesDirectly()
    {
      return false;
    }

    @BeforeAll
    public void setupUsers()
    {
      stateUser = createUser("stateUser", List.of(READ_STATE));
      noPermissionUser = createUser("noPermissionUser", List.of());
    }

    /**
     * SQL on the Druid engine needs only STATE READ.
     */
    @Test
    public void testSqlRequiresStateRead()
    {
      final String sql = brokerStackTraceSql();

      assertPlannedOnDruidEngine(sql);

      final String rows = sql(stateUser, sql, HttpResponseStatus.OK).getContent();
      Assertions.assertTrue(rows.contains(brokerHost()), rows);

      final String forbidden = sql(noPermissionUser, sql, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains(INSUFFICIENT_PERMISSION), forbidden);
    }

    /**
     * A native query needs only STATE READ, on the Broker and on any other service.
     */
    @Test
    public void testNativeQueryRequiresStateRead()
    {
      for (final EmbeddedDruidServer<?> server : servers()) {
        final String host = server.bindings().selfNode().getHostAndPortToUse();
        final StatusResponseHolder rows = nativeQuery(server, stateUser, HttpResponseStatus.OK);
        Assertions.assertTrue(rows.getContent().contains(host), rows.getContent());

        final StatusResponseHolder forbidden = nativeQuery(server, noPermissionUser, HttpResponseStatus.FORBIDDEN);
        Assertions.assertTrue(forbidden.getContent().contains(INSUFFICIENT_PERMISSION), forbidden.getContent());
      }
    }
  }

  /**
   * Cluster and helpers for the {@code sys.stack_trace} authorization tests: a Broker with basic authentication and
   * the Druid engine enabled for system tables, queried with SQL and native queries as users with given permissions.
   */
  abstract static class AuthTestBase extends EmbeddedClusterTestBase
  {
    protected static final String INSUFFICIENT_PERMISSION = "Insufficient permission to view servers";
    protected static final ResourceAction READ_STACK_TRACE_TABLE =
        new ResourceAction(new Resource(StackTraceTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
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
     * Returns the services that native queries are sent to: the Broker, and the Coordinator as a non-Broker service.
     */
    protected List<EmbeddedDruidServer<?>> servers()
    {
      return List.of(broker, coordinator);
    }

    /**
     * Returns the Broker's server name, as {@code sys.stack_trace} reports it.
     */
    protected String brokerHost()
    {
      return broker.bindings().selfNode().getHostAndPortToUse();
    }

    /**
     * Returns SQL that reads the Broker's stack traces.
     */
    protected String brokerStackTraceSql()
    {
      return StringUtils.format(
          """
          SELECT server, thread_name
          FROM sys.stack_trace
          WHERE server = '%s'
          """,
          brokerHost()
      );
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
     * Checks, as the admin, that the Broker plans the SQL on the Druid engine.
     */
    protected void assertPlannedOnDruidEngine(final String sql)
    {
      final String plan = sql(adminClient, "EXPLAIN PLAN FOR " + sql, HttpResponseStatus.OK).getContent();
      Assertions.assertTrue(plan.contains("systemTable"), plan);
    }

    /**
     * Sends a native Scan on {@code sys.stack_trace} to the given service, and checks the response status.
     */
    protected StatusResponseHolder nativeQuery(
        final EmbeddedDruidServer<?> server,
        final HttpClient client,
        final HttpResponseStatus expectedStatus
    )
    {
      final ScanQuery query = Druids.newScanQueryBuilder()
                                    .dataSource(new SystemTableDataSource(StackTraceTableDescriptor.TABLE_NAME))
                                    .eternityInterval()
                                    .columns("server", "thread_name")
                                    .build();
      return HttpUtil.makeRequest(
          client,
          HttpMethod.POST,
          getServerUrl(server) + "/druid/v2/",
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
}
