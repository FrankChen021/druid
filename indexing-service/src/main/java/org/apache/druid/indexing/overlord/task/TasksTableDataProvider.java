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

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.http.TaskStateLookup;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.server.system.table.SystemTableDataProvider;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Native row supplier for {@code sys.tasks}, served by the leader Overlord.
 */
public class TasksTableDataProvider implements SystemTableDataProvider
{
  /**
   * Task storage column for each {@code sys.tasks} column that task storage can filter on.
   */
  private static final Map<String, String> STORAGE_COLUMNS = Map.of(
      "task_id", "id",
      "group_id", "group_id",
      "type", "type",
      "datasource", "datasource",
      "status", "status"
  );

  private final TaskQueryTool taskQueryTool;
  private final TaskMaster taskMaster;

  @Inject
  public TasksTableDataProvider(
      final TaskQueryTool taskQueryTool,
      final TaskMaster taskMaster
  )
  {
    this.taskQueryTool = taskQueryTool;
    this.taskMaster = taskMaster;
  }

  /**
   * Returns the columns task storage can filter on. Without storage support, only {@code status} is used, to choose
   * which of the active and complete task lookups run.
   */
  @Override
  public Set<String> getPushdownColumns()
  {
    return taskQueryTool.supportsTaskStatusQueryFilter() ? STORAGE_COLUMNS.keySet() : Set.of("status");
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters)
  {
    if (!taskMaster.isHalfOrFullLeader()) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("sys.tasks must be read from the current Overlord leader");
    }
    final List<TaskStatusPlus> tasks = taskQueryTool.getTaskStatusPlusList(
        TaskStateLookup.ALL,
        new TaskStorageQueryFilter(toStorageFilters(filters, getPushdownColumns()))
    );
    return Iterables.transform(tasks, TasksTableDataProvider::taskToRow);
  }

  /**
   * Converts the filters that match string values of a pushdown column into task storage filters on the column's
   * storage name. Other filters are left to the query, which applies its whole filter to the returned rows.
   */
  static List<DimFilter> toStorageFilters(final List<Filter> filters, final Set<String> pushdownColumns)
  {
    final List<DimFilter> storageFilters = new ArrayList<>();
    for (final Filter filter : filters) {
      final StringValues stringValues = toStringValues(filter);
      if (stringValues != null && pushdownColumns.contains(stringValues.column)) {
        storageFilters.add(new InDimFilter(STORAGE_COLUMNS.get(stringValues.column), stringValues.values, null));
      }
    }
    return storageFilters;
  }

  /**
   * Returns the column and values of a filter that matches rows whose column is one of a set of non-null strings, or
   * null for any other filter.
   */
  @Nullable
  private static StringValues toStringValues(final Filter filter)
  {
    return switch (filter) {
      case SelectorFilter selector -> selector.getValue() == null
                                      ? null
                                      : new StringValues(selector.getDimension(), Set.of(selector.getValue()));
      case EqualityFilter equality -> ColumnType.STRING.equals(equality.getMatchValueType())
                                      && equality.getMatchValue() instanceof String value
                                      ? new StringValues(equality.getColumn(), Set.of(value))
                                      : null;
      case InDimFilter in -> in.getExtractionFn() == null
                             && !in.getValues().isEmpty()
                             && in.getValues().stream().allMatch(Objects::nonNull)
                             ? new StringValues(in.getDimension(), Set.copyOf(in.getValues()))
                             : null;
      case TypedInFilter in -> ColumnType.STRING.equals(in.getMatchValueType())
                               && !in.getSortedValues().isEmpty()
                               && in.getSortedValues().stream().allMatch(String.class::isInstance)
                               ? new StringValues(
                                   in.getColumn(),
                                   Set.copyOf(in.getSortedValues().stream().map(String.class::cast).toList())
                               )
                               : null;
      case OrFilter or -> toStringValues(or);
      default -> null;
    };
  }

  /**
   * Returns the union of an OR of string-values filters on one column, or null otherwise.
   */
  @Nullable
  private static StringValues toStringValues(final OrFilter or)
  {
    String column = null;
    final Set<String> values = new HashSet<>();
    for (final Filter field : or.getFilters()) {
      final StringValues fieldValues = toStringValues(field);
      if (fieldValues == null || (column != null && !column.equals(fieldValues.column))) {
        return null;
      }
      column = fieldValues.column;
      values.addAll(fieldValues.values);
    }
    return column == null ? null : new StringValues(column, values);
  }

  private record StringValues(String column, Set<String> values)
  {
  }

  private static Object[] taskToRow(final TaskStatusPlus task)
  {
    return new Object[]{
        task.getId(),
        task.getGroupId(),
        task.getType(),
        task.getDataSource(),
        task.getCreatedTime() == null ? null : task.getCreatedTime().toString(),
        task.getQueueInsertionTime() == null ? null : task.getQueueInsertionTime().toString(),
        task.getStatusCode() == null ? null : task.getStatusCode().toString(),
        task.getRunnerStatusCode() == null ? null : task.getRunnerStatusCode().toString(),
        task.getDuration() == null ? 0L : task.getDuration(),
        task.getLocation().getLocation(),
        task.getLocation().getHost(),
        (long) task.getLocation().getPort(),
        (long) task.getLocation().getTlsPort(),
        task.getErrorMsg()
    };
  }
}
