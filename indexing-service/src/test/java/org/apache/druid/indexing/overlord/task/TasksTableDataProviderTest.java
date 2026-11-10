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

package org.apache.druid.indexing.overlord.task;

import com.google.common.base.Optional;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.http.TaskStateLookup;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TasksTableDataProviderTest
{
  private static final Set<String> ALL_PUSHDOWN_COLUMNS = Set.of("task_id", "group_id", "type", "datasource", "status");

  /**
   * Only status is pushed down when the task storage can't filter; it narrows the active and complete lookups.
   */
  @Test
  public void testPushdownColumnsDependOnStorageSupport()
  {
    final TaskQueryTool supportingStorage = Mockito.mock(TaskQueryTool.class);
    Mockito.when(supportingStorage.supportsTaskStatusQueryFilter()).thenReturn(true);

    Assertions.assertEquals(ALL_PUSHDOWN_COLUMNS, new TasksTableDataProvider(supportingStorage, null).getPushdownColumns());
    Assertions.assertEquals(
        Set.of("status"),
        new TasksTableDataProvider(Mockito.mock(TaskQueryTool.class), null).getPushdownColumns()
    );
  }

  @Test
  public void testPassesStorageFiltersToTaskQueryTool()
  {
    final TaskQueryTool taskQueryTool = Mockito.mock(TaskQueryTool.class);
    Mockito.when(taskQueryTool.supportsTaskStatusQueryFilter()).thenReturn(true);
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    Mockito.when(
        taskQueryTool.getTaskStatusPlusList(Mockito.eq(TaskStateLookup.ALL), Mockito.any(TaskStorageQueryFilter.class))
    ).thenReturn(Collections.emptyList());

    new TasksTableDataProvider(taskQueryTool, taskMaster).getRows(
        List.of(new SelectorDimFilter("task_id", "task-a", null).toFilter())
    );

    final ArgumentCaptor<TaskStorageQueryFilter> filtersCaptor = ArgumentCaptor.forClass(TaskStorageQueryFilter.class);
    Mockito.verify(taskQueryTool).getTaskStatusPlusList(Mockito.eq(TaskStateLookup.ALL), filtersCaptor.capture());
    Assertions.assertEquals(
        List.of(new InDimFilter("id", Set.of("task-a"), null)),
        filtersCaptor.getValue().getFilters()
    );
  }

  /**
   * A standby Overlord rejects {@code sys.tasks} instead of returning an incorrectly empty table.
   */
  @Test
  public void testRejectsRowsOnStandbyOverlord()
  {
    final TasksTableDataProvider provider = new TasksTableDataProvider(
        Mockito.mock(TaskQueryTool.class),
        Mockito.mock(TaskMaster.class)
    );

    assertServiceUnavailable(() -> provider.getRows(Collections.emptyList()));
  }

  /**
   * A leadership loss before the task-runner snapshot fails instead of returning an incorrectly empty table.
   */
  @Test
  public void testRejectsRowsWhenTaskRunnerDisappearsDuringLeaderTransition()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    Mockito.when(taskMaster.getTaskRunner()).thenReturn(Optional.absent());
    final TaskQueryTool taskQueryTool = new TaskQueryTool(
        Mockito.mock(TaskStorage.class),
        Mockito.mock(GlobalTaskLockbox.class),
        taskMaster,
        null
    );

    assertServiceUnavailable(() -> new TasksTableDataProvider(taskQueryTool, taskMaster).getRows(List.of()));
  }

  /**
   * Losing the selected task runner while reading also fails instead of returning a stale or empty table.
   */
  @Test
  public void testRejectsRowsWhenTaskRunnerDisappearsAfterSnapshot()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final TaskRunner taskRunner = Mockito.mock(TaskRunner.class);
    final TaskStorage taskStorage = Mockito.mock(TaskStorage.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    Mockito.when(taskMaster.getTaskRunner()).thenReturn(Optional.of(taskRunner), Optional.absent());
    Mockito.when(taskRunner.getKnownTasks()).thenReturn(Collections.emptyList());
    Mockito.when(
        taskStorage.getTaskStatusPlusListWithFilter(Mockito.anyMap(), Mockito.any(TaskStorageQueryFilter.class))
    ).thenReturn(Collections.emptyList());
    final TaskQueryTool taskQueryTool = new TaskQueryTool(
        taskStorage,
        Mockito.mock(GlobalTaskLockbox.class),
        taskMaster,
        null
    );

    assertServiceUnavailable(() -> new TasksTableDataProvider(taskQueryTool, taskMaster).getRows(List.of()));
  }

  /**
   * An OR of values on one column becomes one storage filter with every value.
   */
  @Test
  public void testConvertsOrOfOneColumnToOneStorageFilter()
  {
    final Filter filter = new OrDimFilter(
        new SelectorDimFilter("datasource", "native_sys_a", null),
        new EqualityFilter("datasource", ColumnType.STRING, "native_sys_b", null)
    ).toFilter();

    Assertions.assertEquals(
        List.of(new InDimFilter("datasource", Set.of("native_sys_a", "native_sys_b"), null)),
        TasksTableDataProvider.toStorageFilters(List.of(filter), ALL_PUSHDOWN_COLUMNS)
    );
  }

  /**
   * Filters that don't match a set of string values, or that read a column task storage can't filter on, are left to
   * the query.
   */
  @Test
  public void testSkipsFiltersTaskStorageCannotApply()
  {
    final List<Filter> filters = List.of(
        new OrDimFilter(
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new SelectorDimFilter("type", "noop", null)
        ).toFilter(),
        new SelectorDimFilter("group_id", null, null).toFilter(),
        new LikeDimFilter("task_id", "native_sys_%", null, null).toFilter(),
        new RangeFilter("datasource", ColumnType.STRING, "a", "b", false, true, null),
        new SelectorDimFilter("error_msg", "failed", null).toFilter()
    );

    Assertions.assertEquals(List.of(), TasksTableDataProvider.toStorageFilters(filters, ALL_PUSHDOWN_COLUMNS));
    Assertions.assertEquals(
        List.of(),
        TasksTableDataProvider.toStorageFilters(
            List.of(new SelectorDimFilter("datasource", "native_sys_a", null).toFilter()),
            Set.of("status")
        )
    );
  }

  @Test
  public void testConvertsEveryStorageBackedColumn()
  {
    final List<Filter> filters = List.of(
        new TypedInFilter("task_id", ColumnType.STRING, List.of("task-a", "task-b"), null, null),
        new SelectorDimFilter("group_id", "group-a", null).toFilter(),
        new InDimFilter("type", Set.of("noop"), null),
        new EqualityFilter("datasource", ColumnType.STRING, "native_sys_a", null),
        new SelectorDimFilter("status", "SUCCESS", null).toFilter()
    );

    final List<DimFilter> storageFilters = TasksTableDataProvider.toStorageFilters(filters, ALL_PUSHDOWN_COLUMNS);
    final TaskStorageQueryFilter taskStorageQueryFilter = new TaskStorageQueryFilter(storageFilters);

    Assertions.assertEquals(
        List.of(
            new InDimFilter("id", Set.of("task-a", "task-b"), null),
            new InDimFilter("group_id", Set.of("group-a"), null),
            new InDimFilter("type", Set.of("noop"), null),
            new InDimFilter("datasource", Set.of("native_sys_a"), null),
            new InDimFilter("status", Set.of("SUCCESS"), null)
        ),
        storageFilters
    );
    Assertions.assertFalse(taskStorageQueryFilter.includesActiveTasks());
    Assertions.assertTrue(taskStorageQueryFilter.includesCompleteTasks());
  }

  private static void assertServiceUnavailable(final Executable executable)
  {
    final DruidException e = Assertions.assertThrows(DruidException.class, executable);
    Assertions.assertEquals(DruidException.Category.SERVICE_UNAVAILABLE, e.getCategory());
  }
}
