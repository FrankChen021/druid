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


package org.apache.druid.testing.embedded.query.sys.server_segments;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
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
import org.apache.druid.server.system.table.ServerSegmentsTableDescriptor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Authorization of {@code sys.server_segments} on the Druid engine, with SQL and native queries, as users with given
 * permissions. A caller needs STATE READ, and sees only the segments of the datasources it has DATASOURCE READ on.
 * Each nested class runs its own cluster, with or without {@code druid.auth.authorizeSystemTablesDirectly}.
 */
public class NativeSysServerSegmentsAuthTest
{
  /**
   * Authorization with {@code druid.auth.authorizeSystemTablesDirectly} set. A caller also needs SYSTEM_TABLE READ on
   * {@code server_segments}, whether it queries the table with SQL or with a native query. The table is read on the
   * Broker that runs the query, which applies the caller's access while reading its own rows.
   */
  @Nested
  public class AuthorizeSystemTablesDirectly extends AuthTestBase
  {
    private HttpClient fullUser;
    private HttpClient noDatasourceUser;
    private HttpClient noStateUser;
    private HttpClient noSysTableUser;

    @Override
    protected boolean authorizeSystemTablesDirectly()
    {
      return true;
    }

    @BeforeAll
    public void setupUsers()
    {
      fullUser = createUser("fullUser", List.of(READ_SERVER_SEGMENTS_TABLE, READ_STATE, READ_ALLOWED_DATASOURCE));
      noDatasourceUser = createUser("noDatasourceUser", List.of(READ_SERVER_SEGMENTS_TABLE, READ_STATE));
      noStateUser = createUser("noStateUser", List.of(READ_SERVER_SEGMENTS_TABLE, READ_ALLOWED_DATASOURCE));
      noSysTableUser = createUser("noSysTableUser", List.of(READ_STATE, READ_ALLOWED_DATASOURCE));
    }

    /**
     * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, and STATE READ, and
     * returns only the segments of datasources the caller may read.
     */
    @Test
    public void testSqlReturnsSegmentsOfReadableDatasources()
    {
      assertPlannedOnDruidEngine(SEGMENT_COUNT_SQL);

      Assertions.assertEquals(
          ALLOWED_DATASOURCE + ",1\n" + OTHER_DATASOURCE + ",1",
          sql(adminClient, SEGMENT_COUNT_SQL, HttpResponseStatus.OK).getContent().trim()
      );
      Assertions.assertEquals(
          ALLOWED_DATASOURCE + ",1",
          sql(fullUser, SEGMENT_COUNT_SQL, HttpResponseStatus.OK).getContent().trim()
      );
      Assertions.assertEquals("", sql(noDatasourceUser, SEGMENT_COUNT_SQL, HttpResponseStatus.OK).getContent().trim());

      final String forbidden = sql(noStateUser, SEGMENT_COUNT_SQL, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);

      final String invisible = sql(noSysTableUser, SEGMENT_COUNT_SQL, HttpResponseStatus.BAD_REQUEST).getContent();
      Assertions.assertTrue(invisible.contains("Object 'server_segments' not found within 'sys'"), invisible);
    }

    /**
     * A native query on the Broker needs the same permissions, and returns only the segments of datasources the
     * caller may read.
     */
    @Test
    public void testNativeQueryReturnsSegmentsOfReadableDatasources()
    {
      assertOnlyAllowedDatasource(nativeQuery(fullUser, HttpResponseStatus.OK).getContent());
      assertNoDatasource(nativeQuery(noDatasourceUser, HttpResponseStatus.OK).getContent());

      final String forbidden = nativeQuery(noStateUser, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);

      final String invisible = nativeQuery(noSysTableUser, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(invisible.contains("Unauthorized"), invisible);
    }
  }

  /**
   * Authorization without {@code druid.auth.authorizeSystemTablesDirectly}, the default. System tables then have no
   * table-level check, so STATE READ and DATASOURCE READ let a caller read the segments of that datasource, with SQL
   * or with a native query. A caller without STATE READ is rejected.
   */
  @Nested
  public class DefaultAuthorization extends AuthTestBase
  {
    private HttpClient stateWithDatasourceUser;
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
      stateWithDatasourceUser = createUser("stateWithDatasourceUser", List.of(READ_STATE, READ_ALLOWED_DATASOURCE));
      stateUser = createUser("stateUser", List.of(READ_STATE));
      noPermissionUser = createUser("noPermissionUser", List.of());
    }

    /**
     * SQL on the Druid engine needs STATE READ, and returns only the segments of datasources the caller may read.
     */
    @Test
    public void testSqlReturnsSegmentsOfReadableDatasources()
    {
      assertPlannedOnDruidEngine(SEGMENT_COUNT_SQL);

      Assertions.assertEquals(
          ALLOWED_DATASOURCE + ",1",
          sql(stateWithDatasourceUser, SEGMENT_COUNT_SQL, HttpResponseStatus.OK).getContent().trim()
      );
      Assertions.assertEquals("", sql(stateUser, SEGMENT_COUNT_SQL, HttpResponseStatus.OK).getContent().trim());

      final String forbidden = sql(noPermissionUser, SEGMENT_COUNT_SQL, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);
    }

    /**
     * A native query on the Broker needs STATE READ, and returns only the segments of datasources the caller may read.
     */
    @Test
    public void testNativeQueryReturnsSegmentsOfReadableDatasources()
    {
      assertOnlyAllowedDatasource(nativeQuery(stateWithDatasourceUser, HttpResponseStatus.OK).getContent());
      assertNoDatasource(nativeQuery(stateUser, HttpResponseStatus.OK).getContent());

      final String forbidden = nativeQuery(noPermissionUser, HttpResponseStatus.FORBIDDEN).getContent();
      Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);
    }
  }

  /**
   * Cluster and helpers for the {@code sys.server_segments} authorization tests: a cluster with basic authentication
   * and the Druid engine enabled for system tables, holding one segment in each of two datasources, queried on the
   * Broker with SQL and native queries as users with given permissions.
   */
  abstract static class AuthTestBase extends EmbeddedClusterTestBase
  {
    protected static final String ALLOWED_DATASOURCE = "allowed_datasource";
    protected static final String OTHER_DATASOURCE = "other_datasource";
    protected static final String SEGMENT_COUNT_SQL =
        "SELECT datasource, COUNT(*) FROM sys.server_segments GROUP BY 1 ORDER BY 1";
    protected static final ResourceAction READ_SERVER_SEGMENTS_TABLE = new ResourceAction(
        new Resource(ServerSegmentsTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE),
        Action.READ
    );
    protected static final ResourceAction READ_STATE =
        new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ);
    protected static final ResourceAction READ_ALLOWED_DATASOURCE =
        new ResourceAction(new Resource(ALLOWED_DATASOURCE, ResourceType.DATASOURCE), Action.READ);
    private static final String PASSWORD = "helloworld";

    private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
    private final EmbeddedOverlord overlord = new EmbeddedOverlord();
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
                                 .addServer(overlord)
                                 .addServer(new EmbeddedIndexer())
                                 .addServer(new EmbeddedHistorical())
                                 .addServer(broker);
    }

    @BeforeAll
    public void setupAdminAndData()
    {
      adminClient = client(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD);
      ingest(ALLOWED_DATASOURCE);
      ingest(OTHER_DATASOURCE);
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
     * Sends a native Scan on {@code sys.server_segments} to the Broker, and checks the response status.
     */
    protected StatusResponseHolder nativeQuery(final HttpClient client, final HttpResponseStatus expectedStatus)
    {
      final ScanQuery query = Druids.newScanQueryBuilder()
                                    .dataSource(
                                        new SystemTableDataSource(ServerSegmentsTableDescriptor.TABLE_NAME)
                                    )
                                    .eternityInterval()
                                    .columns("server", "segment_id", "datasource")
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
     * Checks that a native query's rows hold the segments of the allowed datasource only.
     */
    protected static void assertOnlyAllowedDatasource(final String rows)
    {
      Assertions.assertTrue(rows.contains(ALLOWED_DATASOURCE), rows);
      Assertions.assertFalse(rows.contains(OTHER_DATASOURCE), rows);
    }

    /**
     * Checks that a native query returned no segment of either datasource.
     */
    protected static void assertNoDatasource(final String rows)
    {
      Assertions.assertFalse(rows.contains(ALLOWED_DATASOURCE), rows);
      Assertions.assertFalse(rows.contains(OTHER_DATASOURCE), rows);
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

    private void ingest(final String dataSource)
    {
      final Task task = MoreResources.Task.BASIC_INDEX
          .get()
          .segmentGranularity("YEAR")
          .dataSource(dataSource)
          .withId(IdUtils.getRandomId());
      cluster.callApi().runTask(task, overlord);
      cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    }

    private HttpClient client(final String user, final String password)
    {
      return new CredentialedHttpClient(new BasicCredentials(user, password), broker.bindings().globalHttpClient());
    }
  }
}
