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
 * Authorization of {@code sys.servers} on the Druid engine without {@code druid.auth.authorizeSystemTablesDirectly},
 * the default. System tables then have no table-level check, so STATE READ alone lets a caller read the table, with
 * SQL or with a native query.
 */
public class NativeSysServersStateReadAuthTest extends NativeSysServersAuthTestBase
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
    final String sql = "SELECT server, server_type FROM sys.servers WHERE server_type = 'broker'";

    assertPlannedOnDruidEngine(sql);

    final String rows = sql(stateUser, sql, HttpResponseStatus.OK).getContent();
    Assertions.assertTrue(rows.contains(brokerServer() + ",broker"), rows);

    final String forbidden = sql(noPermissionUser, sql, HttpResponseStatus.FORBIDDEN).getContent();
    Assertions.assertTrue(forbidden.contains("Insufficient permission to view servers"), forbidden);
  }

  /**
   * A native query on the Broker needs only STATE READ.
   */
  @Test
  public void testNativeQueryRequiresStateRead()
  {
    final StatusResponseHolder rows = nativeQuery(stateUser, HttpResponseStatus.OK);
    Assertions.assertTrue(rows.getContent().contains(brokerServer()), rows.getContent());

    final StatusResponseHolder forbidden = nativeQuery(noPermissionUser, HttpResponseStatus.FORBIDDEN);
    Assertions.assertTrue(
        forbidden.getContent().contains("Insufficient permission to view servers"),
        forbidden.getContent()
    );
  }
}
