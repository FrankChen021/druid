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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Authorization of {@code sys.servers} on the Druid engine, with {@code druid.auth.authorizeSystemTablesDirectly}
 * set. A caller needs SYSTEM_TABLE READ on {@code servers} and STATE READ, whether it queries the table with SQL or
 * with a native query. The table is read on the Broker that runs the query, which applies the caller's access while
 * reading its own rows.
 */
public class NativeSysServersAuthTest extends NativeSysServersAuthTestBase
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

    assertPlannedOnDruidEngine(sql);

    final String rows = sql(sysTableWithStateUser, sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(rows.contains(brokerServer() + ",broker"), rows);

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
        rows.getContent().contains(brokerServer()),
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
}
