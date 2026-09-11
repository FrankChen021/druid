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

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.QueryInfoAndReport;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.StreamSupport;

public class DartSystemTableQueryInfoProviderTest
{
  @Test
  public void testMapsLocalControllerAsInitialQuery()
  {
    final DartQueryInfo dartQueryInfo = new DartQueryInfo(
        "sql-1",
        "dart-1",
        "SELECT 1",
        "broker:8082",
        "allow",
        "alice",
        DateTimes.of("2026-01-01"),
        "RUNNING",
        null
    );
    final DartControllerRegistry registry = EasyMock.createMock(DartControllerRegistry.class);
    EasyMock.expect(registry.getAllQueryDetails(true)).andReturn(
        List.of(new QueryInfoAndReport(dartQueryInfo, null, DateTimes.of("2026-01-01")))
    );
    EasyMock.replay(registry);
    final DartSystemTableQueryInfoProvider provider = new DartSystemTableQueryInfoProvider(
        registry,
        TestHelper.makeJsonMapper()
    );

    final List<SystemTableQueryInfo> rows = StreamSupport.stream(
        provider.getQueryInfo().spliterator(),
        false
    ).toList();

    Assertions.assertEquals(1, rows.size());
    final SystemTableQueryInfo row = rows.get(0);
    Assertions.assertEquals("dart-1", row.id());
    Assertions.assertEquals("sql-1", row.initialQueryId());
    Assertions.assertTrue(row.initialQuery());
    Assertions.assertEquals("broker:8082", row.server());
    Assertions.assertEquals("broker", row.serverType());
    Assertions.assertTrue(row.info().contains("\"sqlQueryId\":\"sql-1\""));
    EasyMock.verify(registry);
  }
}
