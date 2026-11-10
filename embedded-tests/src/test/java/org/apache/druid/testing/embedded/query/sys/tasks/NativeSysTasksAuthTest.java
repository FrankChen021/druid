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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * Authorization of {@code sys.tasks} on the Druid engine, with {@code druid.auth.authorizeSystemTablesDirectly} set.
 * A caller needs SYSTEM_TABLE READ on {@code tasks} to read the table, with SQL or with a native query, and sees only
 * the tasks of datasources it may read.
 */
public class NativeSysTasksAuthTest extends NativeSysTasksAuthTestBase
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
   * A native query on the Overlord needs SYSTEM_TABLE READ too, and shows only the tasks of datasources the caller may
   * read.
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
