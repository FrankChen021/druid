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

package org.apache.druid.testing.embedded.query.sys.segments;

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
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Authorization of {@code sys.segments} on the Druid engine, with {@code druid.auth.authorizeSystemTablesDirectly}
 * set. A caller needs SYSTEM_TABLE READ on {@code segments} to query the table with SQL or with a native query, and
 * sees only the segments of the datasources it has DATASOURCE READ on. The table is read on the Broker that runs the
 * query, which applies the caller's access while reading its own rows.
 */
public class NativeSysSegmentsAuthTest extends EmbeddedClusterTestBase
{
  private static final String PASSWORD = "helloworld";
  private static final String ALLOWED_DATASOURCE = "allowed_datasource";
  private static final String OTHER_DATASOURCE = "other_datasource";
  private static final ResourceAction READ_SEGMENTS_TABLE =
      new ResourceAction(new Resource(SegmentsTableDescriptor.TABLE_NAME, ResourceType.SYSTEM_TABLE), Action.READ);
  private static final ResourceAction READ_ALLOWED_DATASOURCE =
      new ResourceAction(new Resource(ALLOWED_DATASOURCE, ResourceType.DATASOURCE), Action.READ);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private HttpClient adminClient;
  private HttpClient sysTableWithDatasourceUser;
  private HttpClient sysTableUser;
  private HttpClient datasourceUser;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addResource(new EmbeddedBasicAuthResource())
                               .addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true")
                               .addCommonProperty("druid.auth.authorizeSystemTablesDirectly", "true")
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedHistorical())
                               .addServer(broker);
  }

  @BeforeAll
  public void setupDataAndUsers()
  {
    ingest(ALLOWED_DATASOURCE);
    ingest(OTHER_DATASOURCE);

    adminClient = client(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD);
    sysTableWithDatasourceUser = createUser(
        "sysTableWithDatasourceUser",
        List.of(READ_SEGMENTS_TABLE, READ_ALLOWED_DATASOURCE)
    );
    sysTableUser = createUser("sysTableUser", List.of(READ_SEGMENTS_TABLE));
    datasourceUser = createUser("datasourceUser", List.of(READ_ALLOWED_DATASOURCE));
  }

  /**
   * SQL on the Druid engine needs SYSTEM_TABLE READ, without which the table isn't visible, and returns only the
   * segments of datasources the caller may read.
   */
  @Test
  public void testSqlReturnsSegmentsOfReadableDatasources()
  {
    final String sql = "SELECT datasource, COUNT(*) FROM sys.segments GROUP BY 1 ORDER BY 1";

    final String plan = sql(adminClient, "EXPLAIN PLAN FOR " + sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(plan.contains("systemTable"), plan);

    Assertions.assertEquals(
        ALLOWED_DATASOURCE + ",1\n" + OTHER_DATASOURCE + ",1",
        sql(adminClient, sql, HttpResponseStatus.OK).getContent().trim()
    );
    Assertions.assertEquals(
        ALLOWED_DATASOURCE + ",1",
        sql(sysTableWithDatasourceUser, sql, HttpResponseStatus.OK).getContent().trim()
    );
    Assertions.assertEquals("", sql(sysTableUser, sql, HttpResponseStatus.OK).getContent().trim());

    final String invisible = sql(datasourceUser, sql, HttpResponseStatus.BAD_REQUEST).getContent();
    Assertions.assertTrue(invisible.contains("Object 'segments' not found within 'sys'"), invisible);
  }

  /**
   * A native query on the Broker needs SYSTEM_TABLE READ, and returns only the segments of datasources the caller may
   * read.
   */
  @Test
  public void testNativeQueryReturnsSegmentsOfReadableDatasources()
  {
    final String rows = nativeQuery(sysTableWithDatasourceUser, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(rows.contains(ALLOWED_DATASOURCE), rows);
    Assertions.assertFalse(rows.contains(OTHER_DATASOURCE), rows);

    final String noRows = nativeQuery(sysTableUser, HttpResponseStatus.OK).getContent();
    Assertions.assertFalse(noRows.contains(ALLOWED_DATASOURCE), noRows);
    Assertions.assertFalse(noRows.contains(OTHER_DATASOURCE), noRows);

    nativeQuery(datasourceUser, HttpResponseStatus.FORBIDDEN);
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
                                  .dataSource(new SystemTableDataSource(SegmentsTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .columns("segment_id", "datasource")
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
