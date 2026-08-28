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

package org.apache.druid.indexing.overlord.supervisor;

import com.google.common.base.Optional;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SupervisorsTableDataProviderTest
{
  @Test
  public void testReturnsSystemRowsAndPushesDownSupervisorId() throws Exception
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final SupervisorManager manager = Mockito.mock(SupervisorManager.class);
    final SupervisorSpec supervisorA = new NoopSupervisorSpec("supervisor-a", List.of("datasource-a"));
    final SupervisorSpec supervisorB = new NoopSupervisorSpec("supervisor-b", List.of("datasource-b"));
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.of(manager));
    Mockito.when(manager.getSupervisorIds()).thenReturn(Set.of("supervisor-a", "supervisor-b"));
    Mockito.when(manager.getSupervisorState("supervisor-a"))
           .thenReturn(Optional.of(SupervisorStateManager.BasicState.RUNNING));
    Mockito.when(manager.getSupervisorSpec("supervisor-a")).thenReturn(Optional.of(supervisorA));
    Mockito.when(manager.getSupervisorSpec("supervisor-b")).thenReturn(Optional.of(supervisorB));
    final SupervisorsTableDataProvider provider = new SupervisorsTableDataProvider(
        taskMaster,
        TestHelper.makeJsonMapper()
    );
    final List<DimFilter> filters = List.of(
        new SelectorDimFilter("supervisor_id", "supervisor-a", null)
    );

    final List<Object[]> rows = toList(provider.getRows(filters, Mockito.mock(AuthenticationResult.class)));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(
        new Object[]{
            "supervisor-a",
            "datasource-a",
            "RUNNING",
            "RUNNING",
            1L,
            "noop",
            "noop",
            0L,
            TestHelper.makeJsonMapper().writeValueAsString(supervisorA)
        },
        rows.get(0)
    );
    Mockito.verify(manager, Mockito.never()).getSupervisorState("supervisor-b");
  }

  @Test
  public void testRejectsRowsOnStandbyOverlord()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.absent());
    final SupervisorsTableDataProvider provider = new SupervisorsTableDataProvider(
        taskMaster,
        TestHelper.makeJsonMapper()
    );

    Assertions.assertThrows(
        SystemTableNotLeaderException.class,
        () -> provider.getRows(Collections.emptyList(), Mockito.mock(AuthenticationResult.class))
    );
  }

  private static List<Object[]> toList(final Iterable<Object[]> rows)
  {
    final List<Object[]> list = new ArrayList<>();
    rows.forEach(list::add);
    return list;
  }
}
