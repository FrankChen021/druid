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

package org.apache.druid.server.system;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.system.table.QuerySchedulerQueryInfoProvider;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

public class QuerySchedulerQueryInfoProviderTest
{
  @Test
  public void testUsesSqlQueryIdAsInitialQueryIdOnBroker()
  {
    final QueryScheduler scheduler = EasyMock.createMock(QueryScheduler.class);
    EasyMock.expect(scheduler.getRunningQueryInfo()).andReturn(
        List.of(
            new QueryScheduler.RegisteredQueryInfo(
                "native-1",
                "sql-1",
                null,
                "scan",
                Set.of("foo"),
                null,
                false
            )
        )
    );
    EasyMock.replay(scheduler);
    final QuerySchedulerQueryInfoProvider provider = new QuerySchedulerQueryInfoProvider(
        scheduler,
        new DruidNode("broker", "localhost", false, 8082, null, true, false),
        Set.of(NodeRole.BROKER)
    );

    final SystemTableQueryInfo queryInfo = onlyQuery(provider);

    Assertions.assertEquals("native-1", queryInfo.id());
    Assertions.assertEquals("sql-1", queryInfo.initialQueryId());
    Assertions.assertTrue(queryInfo.initialQuery());
    Assertions.assertEquals("localhost:8082", queryInfo.server());
    Assertions.assertEquals("broker", queryInfo.serverType());
    Assertions.assertEquals(
        Map.of("queryType", "scan", "datasources", List.of("foo"), "sqlQueryId", "sql-1"),
        StructuredData.unwrap(queryInfo.info())
    );
    EasyMock.verify(scheduler);
  }

  @Test
  public void testUsesNativeIdForHistoricalWorkerAndExcludesOnlyExplicitSystemTableRequest()
  {
    final QueryScheduler scheduler = EasyMock.createMock(QueryScheduler.class);
    EasyMock.expect(scheduler.getRunningQueryInfo()).andReturn(
        List.of(
            new QueryScheduler.RegisteredQueryInfo(
                "native-system-node-user-query",
                null,
                null,
                "scan",
                Set.of("foo"),
                null,
                false
            ),
            new QueryScheduler.RegisteredQueryInfo(
                "internal-request",
                "sys-query",
                null,
                "scan",
                Set.of("sys.queries"),
                null,
                true
            )
        )
    );
    EasyMock.replay(scheduler);
    final QuerySchedulerQueryInfoProvider provider = new QuerySchedulerQueryInfoProvider(
        scheduler,
        new DruidNode("historical", "localhost", false, 8083, null, true, false),
        Set.of(NodeRole.HISTORICAL)
    );

    final SystemTableQueryInfo queryInfo = onlyQuery(provider);

    Assertions.assertEquals("native-system-node-user-query", queryInfo.initialQueryId());
    Assertions.assertFalse(queryInfo.initialQuery());
    Assertions.assertEquals("historical", queryInfo.serverType());
    EasyMock.verify(scheduler);
  }

  private static SystemTableQueryInfo onlyQuery(final QuerySchedulerQueryInfoProvider provider)
  {
    final List<SystemTableQueryInfo> queryInfo = StreamSupport.stream(
        provider.getQueryInfo().spliterator(),
        false
    ).toList();
    Assertions.assertEquals(1, queryInfo.size());
    return queryInfo.get(0);
  }
}
