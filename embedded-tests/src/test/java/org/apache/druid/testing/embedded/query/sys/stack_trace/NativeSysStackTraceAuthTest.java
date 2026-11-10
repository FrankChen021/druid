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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Authorization of {@code sys.stack_trace} on the Druid engine, with {@code druid.auth.authorizeSystemTablesDirectly}
 * set. A caller needs SYSTEM_TABLE READ on {@code stack_trace}, STATE READ and CONFIG READ, whether it queries the
 * table with SQL or with a native query.
 */
public class NativeSysStackTraceAuthTest extends EmbeddedClusterTestBase
{
  private static final String PASSWORD = "helloworld";
  private static final String INSUFFICIENT_PERMISSION =
      "Insufficient permission to view thread stack traces, which requires STATE READ and CONFIG READ";
  /**
   * Message of the table-level SYSTEM_TABLE check, which rejects the caller before the row authorizer runs.
   */
  private static final String NO_SYSTEM_TABLE_PERMISSION = "Unauthorized";
  private static final ResourceAction READ_STACK_TRACE_TABLE =
      new ResourceAction(new Resource(StackTraceTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
  private static final ResourceAction READ_STATE =
      new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ);
  private static final ResourceAction READ_CONFIG =
      new ResourceAction(new Resource(".*", ResourceType.CONFIG), Action.READ);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private HttpClient adminClient;
  private HttpClient fullUser;
  private HttpClient noConfigUser;
  private HttpClient noStateUser;
  private HttpClient sysTableUser;
  private HttpClient noSysTableUser;

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
    fullUser = createUser("fullUser", List.of(READ_STACK_TRACE_TABLE, READ_STATE, READ_CONFIG));
    noConfigUser = createUser("noConfigUser", List.of(READ_STACK_TRACE_TABLE, READ_STATE));
    noStateUser = createUser("noStateUser", List.of(READ_STACK_TRACE_TABLE, READ_CONFIG));
    sysTableUser = createUser("sysTableUser", List.of(READ_STACK_TRACE_TABLE));
    noSysTableUser = createUser("noSysTableUser", List.of(READ_STATE, READ_CONFIG));
  }

  /**
   * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, plus STATE READ and
   * CONFIG READ.
   */
  @Test
  public void testSqlRequiresSystemTableStateAndConfigRead()
  {
    final String sql = StringUtils.format(
        """
        SELECT server, thread_name
        FROM sys.stack_trace
        WHERE server = '%s'
        """,
        brokerHost()
    );

    final String plan = sql(fullUser, "EXPLAIN PLAN FOR " + sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(plan.contains("systemTable"), plan);

    final String rows = sql(fullUser, sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(rows.contains(brokerHost()), rows);

    for (final HttpClient forbiddenUser : List.of(noConfigUser, noStateUser, sysTableUser)) {
      final String forbidden = sql(forbiddenUser, sql, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains(INSUFFICIENT_PERMISSION), forbidden);
    }

    final String invisible = sql(noSysTableUser, sql, HttpResponseStatus.BAD_REQUEST).getContent();
    Assertions.assertTrue(invisible.contains("Object 'stack_trace' not found within 'sys'"), invisible);
  }

  /**
   * A native query needs the same three permissions, and no {@code server} filter, on the Broker and on any other
   * service.
   */
  @Test
  public void testNativeQueryRequiresSystemTableStateAndConfigRead()
  {
    for (final EmbeddedDruidServer<?> server : List.<EmbeddedDruidServer<?>>of(broker, coordinator)) {
      final String host = server.bindings().selfNode().getHostAndPortToUse();
      final StatusResponseHolder rows = nativeQuery(server, fullUser, HttpResponseStatus.OK);
      Assertions.assertTrue(rows.getContent().contains(host), rows.getContent());

      for (final HttpClient forbiddenUser : List.of(noConfigUser, noStateUser, sysTableUser)) {
        final StatusResponseHolder forbidden = nativeQuery(server, forbiddenUser, HttpResponseStatus.FORBIDDEN);
        Assertions.assertTrue(forbidden.getContent().contains(INSUFFICIENT_PERMISSION), forbidden.getContent());
      }

      final StatusResponseHolder noSysTable = nativeQuery(server, noSysTableUser, HttpResponseStatus.FORBIDDEN);
      Assertions.assertTrue(noSysTable.getContent().contains(NO_SYSTEM_TABLE_PERMISSION), noSysTable.getContent());
      Assertions.assertFalse(noSysTable.getContent().contains(INSUFFICIENT_PERMISSION), noSysTable.getContent());
    }
  }

  private String brokerHost()
  {
    return broker.bindings().selfNode().getHostAndPortToUse();
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

  private StatusResponseHolder nativeQuery(
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
