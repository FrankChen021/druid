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

package org.apache.druid.testing.embedded.query.sys.tasks;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.indexing.common.task.NoopTask;
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
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
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
import java.util.Set;

/**
 * Authorization of {@code sys.tasks} on the Druid engine, with SQL and native queries, as users with given permissions.
 * Each nested class runs its own cluster, with or without {@code druid.auth.authorizeSystemTablesDirectly}. In both
 * modes, {@code sys.tasks} needs no STATE READ: a caller sees only the tasks of datasources it may read.
 */
public class NativeSysTasksAuthTest
{
  /**
   * Authorization with {@code druid.auth.authorizeSystemTablesDirectly} set. A caller also needs SYSTEM_TABLE READ on
   * {@code tasks} to read the table.
   */
  @Nested
  public class AuthorizeSystemTablesDirectly extends AuthTestBase
  {
    private HttpClient sysTableWithDatasourceUser;
    private HttpClient sysTableUser;
    private HttpClient datasourceUser;

    @Override
    protected boolean authorizeSystemTablesDirectly()
    {
      return true;
    }

    @BeforeAll
    public void setupUsers()
    {
      sysTableWithDatasourceUser =
          createUser("sysTableWithDatasourceUser", List.of(READ_TASKS_TABLE, READ_READABLE_DATASOURCE));
      sysTableUser = createUser("sysTableUser", List.of(READ_TASKS_TABLE));
      datasourceUser = createUser("datasourceUser", List.of(READ_READABLE_DATASOURCE));
    }

    /**
     * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, and shows only the tasks
     * of datasources the caller may read.
     */
    @Test
    public void testSqlRequiresSystemTableReadAndFiltersByDatasource()
    {
      assertPlannedOnDruidEngine(DATASOURCES_SQL);

      Assertions.assertEquals(Set.of(READABLE_DATASOURCE, HIDDEN_DATASOURCE), sqlDatasources(adminClient));
      Assertions.assertEquals(Set.of(READABLE_DATASOURCE), sqlDatasources(sysTableWithDatasourceUser));
      Assertions.assertEquals(Set.of(), sqlDatasources(sysTableUser));

      final String invisible = sql(datasourceUser, DATASOURCES_SQL, HttpResponseStatus.BAD_REQUEST).getContent();
      Assertions.assertTrue(invisible.contains("Object 'tasks' not found within 'sys'"), invisible);
    }

    /**
     * A native query on the Overlord needs SYSTEM_TABLE READ too, and shows only the tasks of datasources the caller
     * may read.
     */
    @Test
    public void testNativeQueryRequiresSystemTableReadAndFiltersByDatasource()
    {
      final String rows = nativeQuery(sysTableWithDatasourceUser, HttpResponseStatus.OK).getContent();
      Assertions.assertTrue(rows.contains(READABLE_TASK), rows);
      Assertions.assertFalse(rows.contains(HIDDEN_TASK), rows);

      final String noDatasourceRows = nativeQuery(sysTableUser, HttpResponseStatus.OK).getContent();
      Assertions.assertFalse(noDatasourceRows.contains(READABLE_TASK), noDatasourceRows);
      Assertions.assertFalse(noDatasourceRows.contains(HIDDEN_TASK), noDatasourceRows);

      final StatusResponseHolder forbidden = nativeQuery(datasourceUser, HttpResponseStatus.FORBIDDEN);
      Assertions.assertTrue(forbidden.getContent().contains("Unauthorized"), forbidden.getContent());
    }
  }

  /**
   * Authorization without {@code druid.auth.authorizeSystemTablesDirectly}, the default. System tables then have no
   * table-level check, so every caller reads the table.
   */
  @Nested
  public class DefaultAuthorization extends AuthTestBase
  {
    private HttpClient datasourceUser;
    private HttpClient noPermissionUser;

    @Override
    protected boolean authorizeSystemTablesDirectly()
    {
      return false;
    }

    @BeforeAll
    public void setupUsers()
    {
      datasourceUser = createUser("datasourceUser", List.of(READ_READABLE_DATASOURCE));
      noPermissionUser = createUser("noPermissionUser", List.of());
    }

    /**
     * SQL on the Druid engine shows only the tasks of datasources the caller may read.
     */
    @Test
    public void testSqlFiltersByDatasource()
    {
      assertPlannedOnDruidEngine(DATASOURCES_SQL);

      Assertions.assertEquals(Set.of(READABLE_DATASOURCE), sqlDatasources(datasourceUser));
      Assertions.assertEquals(Set.of(), sqlDatasources(noPermissionUser));
    }

    /**
     * A native query on the Overlord shows only the tasks of datasources the caller may read.
     */
    @Test
    public void testNativeQueryFiltersByDatasource()
    {
      final String rows = nativeQuery(datasourceUser, HttpResponseStatus.OK).getContent();
      Assertions.assertTrue(rows.contains(READABLE_TASK), rows);
      Assertions.assertFalse(rows.contains(HIDDEN_TASK), rows);

      final String noPermissionRows = nativeQuery(noPermissionUser, HttpResponseStatus.OK).getContent();
      Assertions.assertFalse(noPermissionRows.contains(READABLE_TASK), noPermissionRows);
      Assertions.assertFalse(noPermissionRows.contains(HIDDEN_TASK), noPermissionRows);
    }
  }

  /**
   * Cluster and helpers for the tests: a Broker with basic authentication and the Druid engine enabled for system
   * tables. The cluster has one task in each of two datasources, so that tests can check which tasks a user sees.
   */
  abstract static class AuthTestBase extends EmbeddedClusterTestBase
  {
    protected static final String READABLE_DATASOURCE = "auth_sys_tasks_readable";
    protected static final String HIDDEN_DATASOURCE = "auth_sys_tasks_hidden";
    protected static final String READABLE_TASK = "auth_sys_tasks_readable_task";
    protected static final String HIDDEN_TASK = "auth_sys_tasks_hidden_task";
    protected static final ResourceAction READ_TASKS_TABLE =
        new ResourceAction(new Resource(TaskTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
    protected static final ResourceAction READ_READABLE_DATASOURCE =
        new ResourceAction(new Resource(READABLE_DATASOURCE, ResourceType.DATASOURCE), Action.READ);
    protected static final String DATASOURCES_SQL = """
        SELECT DISTINCT datasource
        FROM sys.tasks
        WHERE datasource IN ('auth_sys_tasks_readable', 'auth_sys_tasks_hidden')
        """;
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
                                 .addServer(broker);
    }

    @BeforeAll
    public void setupAdminAndTasks()
    {
      adminClient = client(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD);
      cluster.callApi().runTask(new NoopTask(READABLE_TASK, null, READABLE_DATASOURCE, 1L, 0L, null), overlord);
      cluster.callApi().runTask(new NoopTask(HIDDEN_TASK, null, HIDDEN_DATASOURCE, 1L, 0L, null), overlord);
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
     * Returns the datasources of the tasks in the cluster that the user sees with SQL.
     */
    protected Set<String> sqlDatasources(final HttpClient client)
    {
      return Set.copyOf(sql(client, DATASOURCES_SQL, HttpResponseStatus.OK).getContent().strip().lines().toList());
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
     * Sends a native Scan on {@code sys.tasks} to the Overlord, which serves the table, and checks the response status.
     * Only SQL gathers system tables from other services, so the Broker doesn't serve native queries on this table.
     */
    protected StatusResponseHolder nativeQuery(final HttpClient client, final HttpResponseStatus expectedStatus)
    {
      final ScanQuery query = Druids.newScanQueryBuilder()
                                    .dataSource(new SystemTableDataSource(TaskTableDescriptor.TABLE_NAME))
                                    .eternityInterval()
                                    .columns("task_id", "datasource")
                                    .build();
      return HttpUtil.makeRequest(
          client,
          HttpMethod.POST,
          getServerUrl(overlord) + "/druid/v2/",
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
