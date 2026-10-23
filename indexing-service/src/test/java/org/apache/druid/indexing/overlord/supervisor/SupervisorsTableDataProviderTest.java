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
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SupervisorsTableDataProviderTest
{
  private static final Set<String> ALL_SUPERVISOR_IDS = Set.of("supervisor-a", "supervisor-b", "supervisor-c");

  /**
   * A filter on supervisor_id builds rows for only the matching supervisors, without reading the state of others.
   */
  @Test
  public void testReturnsSystemRowsAndPushesDownSupervisorId() throws Exception
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final SupervisorManager manager = Mockito.mock(SupervisorManager.class);
    // Distinct type and source, and a detailed state that differs from its basic state, catch swapped columns.
    final SupervisorSpec supervisorA = new NoopSupervisorSpec("supervisor-a", List.of("datasource-a"))
    {
      @Override
      public String getType()
      {
        return "kafka";
      }

      @Override
      public String getSource()
      {
        return "topic-a";
      }
    };
    final SupervisorSpec supervisorB = new NoopSupervisorSpec("supervisor-b", List.of("datasource-b"));
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.of(manager));
    Mockito.when(taskMaster.isFullLeader()).thenReturn(true);
    Mockito.when(manager.getSupervisorIds()).thenReturn(Set.of("supervisor-a", "supervisor-b"));
    Mockito.when(manager.getSupervisorState("supervisor-a"))
           .thenReturn(Optional.of(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM));
    Mockito.when(manager.getSupervisorSpec("supervisor-a")).thenReturn(Optional.of(supervisorA));
    Mockito.when(manager.getSupervisorSpec("supervisor-b")).thenReturn(Optional.of(supervisorB));
    final SupervisorsTableDataProvider provider = new SupervisorsTableDataProvider(
        taskMaster,
        TestHelper.makeJsonMapper()
    );

    final List<Object[]> rows = Lists.newArrayList(
        provider.getRows(List.of(new SelectorDimFilter("supervisor_id", "supervisor-a", null).toFilter()))
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(
        new Object[]{
            "supervisor-a",
            "datasource-a",
            "RUNNING",
            "CONNECTING_TO_STREAM",
            1L,
            "kafka",
            "topic-a",
            0L,
            TestHelper.makeJsonMapper().writeValueAsString(supervisorA)
        },
        rows.getFirst()
    );
    Mockito.verify(manager, Mockito.never()).getSupervisorState("supervisor-b");
    Mockito.verify(manager, Mockito.never()).getSupervisorSpec("supervisor-b");
  }

  /**
   * A supervisor whose spec disappears while the table is read is skipped instead of producing a row without a spec.
   */
  @Test
  public void testSkipsSupervisorsWithoutSpec()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final SupervisorManager manager = Mockito.mock(SupervisorManager.class);
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.of(manager));
    Mockito.when(taskMaster.isFullLeader()).thenReturn(true);
    Mockito.when(manager.getSupervisorIds()).thenReturn(Set.of("supervisor-gone"));
    Mockito.when(manager.getSupervisorSpec("supervisor-gone")).thenReturn(Optional.absent());

    Assertions.assertEquals(
        List.of(),
        Lists.newArrayList(
            new SupervisorsTableDataProvider(taskMaster, TestHelper.makeJsonMapper()).getRows(List.of())
        )
    );
  }

  /**
   * A standby Overlord, or one still becoming leader, has no supervisor manager and rejects {@code sys.supervisors}
   * instead of returning an incorrectly empty table.
   */
  @Test
  public void testRejectsRowsOnStandbyOverlord()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.absent());
    final SupervisorsTableDataProvider provider = new SupervisorsTableDataProvider(
        taskMaster,
        TestHelper.makeJsonMapper()
    );

    assertServiceUnavailable(() -> provider.getRows(Collections.emptyList()));
  }

  /**
   * An Overlord that loses leadership while reading supervisors rejects the table instead of returning partial rows.
   */
  @Test
  public void testRejectsRowsWhenLeadershipIsLostDuringRead()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final SupervisorManager manager = Mockito.mock(SupervisorManager.class);
    Mockito.when(taskMaster.getSupervisorManager()).thenReturn(Optional.of(manager));
    Mockito.when(manager.getSupervisorIds()).thenReturn(Set.of("supervisor-a"));
    Mockito.when(manager.getSupervisorSpec("supervisor-a"))
           .thenReturn(Optional.of(new NoopSupervisorSpec("supervisor-a", List.of("datasource-a"))));
    Mockito.when(manager.getSupervisorState("supervisor-a")).thenReturn(Optional.absent());
    // Leadership is lost only after the supervisors have been read.
    Mockito.when(taskMaster.isFullLeader()).thenReturn(false);

    assertServiceUnavailable(
        () -> new SupervisorsTableDataProvider(taskMaster, TestHelper.makeJsonMapper()).getRows(List.of())
    );
    final InOrder inOrder = Mockito.inOrder(manager, taskMaster);
    inOrder.verify(manager).getSupervisorSpec("supervisor-a");
    inOrder.verify(taskMaster).isFullLeader();
  }

  /**
   * Every supported string-values filter on supervisor_id narrows the supervisors, and filters are intersected.
   */
  @Test
  public void testFilterSupervisorIds()
  {
    Assertions.assertEquals(
        Set.of("supervisor-a", "supervisor-b"),
        SupervisorsTableDataProvider.filterSupervisorIds(
            ALL_SUPERVISOR_IDS,
            List.of(
                new OrDimFilter(
                    new SelectorDimFilter("supervisor_id", "supervisor-a", null),
                    new EqualityFilter("supervisor_id", ColumnType.STRING, "supervisor-b", null)
                ).toFilter()
            )
        )
    );
    Assertions.assertEquals(
        Set.of("supervisor-b"),
        SupervisorsTableDataProvider.filterSupervisorIds(
            ALL_SUPERVISOR_IDS,
            List.of(
                new InDimFilter("supervisor_id", Set.of("supervisor-a", "supervisor-b"), null),
                new TypedInFilter("supervisor_id", ColumnType.STRING, List.of("supervisor-b", "missing"), null, null)
            )
        )
    );
  }

  /**
   * Filters that don't match a set of string values on supervisor_id are left to the query.
   */
  @Test
  public void testFilterSupervisorIdsSkipsOtherFilters()
  {
    final List<Filter> filters = List.of(
        new OrDimFilter(
            new SelectorDimFilter("supervisor_id", "supervisor-a", null),
            new SelectorDimFilter("datasource", "datasource-b", null)
        ).toFilter(),
        new SelectorDimFilter("supervisor_id", null, null).toFilter(),
        new LikeDimFilter("supervisor_id", "supervisor-%", null, null).toFilter(),
        new SelectorDimFilter("datasource", "datasource-a", null).toFilter()
    );

    Assertions.assertEquals(
        ALL_SUPERVISOR_IDS,
        SupervisorsTableDataProvider.filterSupervisorIds(ALL_SUPERVISOR_IDS, filters)
    );
  }

  private static void assertServiceUnavailable(final Executable executable)
  {
    final DruidException e = Assertions.assertThrows(DruidException.class, executable);
    Assertions.assertEquals(DruidException.Category.SERVICE_UNAVAILABLE, e.getCategory());
  }
}
