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

package org.apache.druid.msq.indexing;

import com.google.common.base.Optional;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.query.QueryContext;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

public class MSQTaskSystemTableQueryInfoProviderTest
{
  @Test
  public void testMapsActiveControllerTaskAsInitialQuery()
  {
    final LegacyMSQSpec querySpec = EasyMock.createMock(LegacyMSQSpec.class);
    EasyMock.expect(querySpec.getContext()).andReturn(QueryContext.of(Map.of("sqlQueryId", "sql-1"))).anyTimes();
    final MSQControllerTask controllerTask = EasyMock.createMock(MSQControllerTask.class);
    EasyMock.expect(controllerTask.getQuerySpec()).andReturn(querySpec).once();
    EasyMock.expect(controllerTask.getId()).andReturn("query-sql-1").anyTimes();
    EasyMock.expect(controllerTask.getType()).andReturn(MSQControllerTask.TYPE).once();
    EasyMock.expect(controllerTask.getDataSource()).andReturn("__query_select").once();
    final TaskQueue taskQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskQueue.getActiveTasks()).andReturn(List.of(controllerTask)).once();
    final TaskMaster taskMaster = EasyMock.createMock(TaskMaster.class);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).once();
    EasyMock.replay(querySpec, controllerTask, taskQueue, taskMaster);
    final MSQTaskSystemTableQueryInfoProvider provider = new MSQTaskSystemTableQueryInfoProvider(
        taskMaster,
        TestHelper.makeJsonMapper(),
        new DruidNode("overlord", "localhost", false, 8090, null, true, false),
        Set.of(NodeRole.OVERLORD)
    );

    final List<SystemTableQueryInfo> rows = StreamSupport.stream(
        provider.getQueryInfo().spliterator(),
        false
    ).toList();

    Assertions.assertEquals(1, rows.size());
    final SystemTableQueryInfo row = rows.get(0);
    Assertions.assertEquals("query-sql-1", row.id());
    Assertions.assertEquals("sql-1", row.initialQueryId());
    Assertions.assertTrue(row.initialQuery());
    Assertions.assertEquals("overlord", row.serverType());
    EasyMock.verify(querySpec, controllerTask, taskQueue, taskMaster);
  }

  @Test
  public void testStandbyOverlordReturnsNoRows()
  {
    final TaskMaster taskMaster = EasyMock.createMock(TaskMaster.class);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.absent()).once();
    EasyMock.replay(taskMaster);
    final MSQTaskSystemTableQueryInfoProvider provider = new MSQTaskSystemTableQueryInfoProvider(
        taskMaster,
        TestHelper.makeJsonMapper(),
        new DruidNode("overlord", "localhost", false, 8090, null, true, false),
        Set.of(NodeRole.OVERLORD)
    );

    Assertions.assertFalse(provider.getQueryInfo().iterator().hasNext());
    EasyMock.verify(taskMaster);
  }
}
