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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * Authorization of {@code sys.tasks} on the Druid engine without {@code druid.auth.authorizeSystemTablesDirectly}, the
 * default. System tables then have no table-level check, and {@code sys.tasks} needs no STATE READ either: every
 * caller reads the table, with SQL or with a native query, and sees only the tasks of datasources it may read.
 */
public class NativeSysTasksDatasourceReadAuthTest extends NativeSysTasksAuthTestBase
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
