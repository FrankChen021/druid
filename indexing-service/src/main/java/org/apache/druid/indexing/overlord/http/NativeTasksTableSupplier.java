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

package org.apache.druid.indexing.overlord.http;

import com.google.inject.Inject;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Native row supplier for {@code sys.tasks}. */
public class NativeTasksTableSupplier implements NativeSystemTableDataSupplier
{
  public static final String TABLE_NAME = "tasks";

  private static final List<NativeSystemTableFilterRule> FILTER_RULES = Arrays.asList(
      NativeSystemTableFilterRule.stringValues("task_id"),
      NativeSystemTableFilterRule.stringValues("group_id"),
      NativeSystemTableFilterRule.stringValues("type"),
      NativeSystemTableFilterRule.stringValues("datasource"),
      NativeSystemTableFilterRule.stringValues("created_time"),
      NativeSystemTableFilterRule.lexicographicStringRange("created_time"),
      NativeSystemTableFilterRule.stringValues("status")
  );

  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ColumnType.STRING)
      .add("group_id", ColumnType.STRING)
      .add("type", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("created_time", ColumnType.STRING)
      .add("queue_insertion_time", ColumnType.STRING)
      .add("status", ColumnType.STRING)
      .add("runner_status", ColumnType.STRING)
      .add("duration", ColumnType.LONG)
      .add("location", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("error_msg", ColumnType.STRING)
      .build();

  private final TaskQueryTool taskQueryTool;
  private final TaskMaster taskMaster;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public NativeTasksTableSupplier(
      final TaskQueryTool taskQueryTool,
      final TaskMaster taskMaster,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.taskQueryTool = taskQueryTool;
    this.taskMaster = taskMaster;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  @Override
  public List<NativeSystemTableFilterRule> getFilterRules()
  {
    return FILTER_RULES;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> extractedFilters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    if (!taskMaster.getTaskRunner().isPresent()) {
      return Collections.emptyList();
    }
    final TaskStorageQueryFilter storageFilter = toTaskStorageQueryFilter(extractedFilters);
    final List<TaskStatusPlus> tasks = taskQueryTool.getTaskStatusPlusList(
        TaskStateLookup.ALL,
        storageFilter,
        null,
        null
    );
    final Iterable<TaskStatusPlus> authorizedTasks = AuthorizationUtils.filterAuthorizedResources(
        internalAuthenticationResult,
        tasks,
        task -> Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(task.getDataSource())
        ),
        authorizerMapper
    );
    return StreamSupport.stream(authorizedTasks.spliterator(), false)
                        .map(NativeTasksTableSupplier::taskToRow)
                        .collect(Collectors.toList());
  }

  @Nullable
  static String toDataSourceFilter(final List<DimFilter> extractedFilters)
  {
    return toTaskStorageQueryFilter(extractedFilters).getSingleDataSource();
  }

  static TaskStorageQueryFilter toTaskStorageQueryFilter(final List<DimFilter> extractedFilters)
  {
    final TaskStorageQueryFilter.Builder builder = TaskStorageQueryFilter.builder();
    for (final DimFilter filter : extractedFilters) {
      if (filter instanceof BoundDimFilter) {
        final BoundDimFilter bound = (BoundDimFilter) filter;
        builder.addCreatedTimeRange(
            bound.getLower(),
            bound.isLowerStrict(),
            bound.getUpper(),
            bound.isUpperStrict()
        );
      } else if (filter instanceof RangeFilter) {
        final RangeFilter range = (RangeFilter) filter;
        builder.addCreatedTimeRange(
            (String) range.getLower(),
            range.isLowerOpen(),
            (String) range.getUpper(),
            range.isUpperOpen()
        );
      } else {
        final String column = stringValuesColumn(filter);
        builder.addValues(toStorageColumn(column), stringValues(filter));
      }
    }
    return builder.build();
  }

  private static TaskStorageQueryFilter.Column toStorageColumn(final String column)
  {
    return switch (column) {
      case "task_id" -> TaskStorageQueryFilter.Column.TASK_ID;
      case "group_id" -> TaskStorageQueryFilter.Column.GROUP_ID;
      case "type" -> TaskStorageQueryFilter.Column.TYPE;
      case "datasource" -> TaskStorageQueryFilter.Column.DATASOURCE;
      case "created_time" -> TaskStorageQueryFilter.Column.CREATED_TIME;
      case "status" -> TaskStorageQueryFilter.Column.STATUS;
      default -> throw new IllegalStateException("Unsupported task filter column");
    };
  }

  private static String stringValuesColumn(final DimFilter filter)
  {
    if (filter instanceof SelectorDimFilter) {
      return ((SelectorDimFilter) filter).getDimension();
    } else if (filter instanceof EqualityFilter) {
      return ((EqualityFilter) filter).getColumn();
    } else if (filter instanceof InDimFilter) {
      return ((InDimFilter) filter).getDimension();
    } else if (filter instanceof TypedInFilter) {
      return ((TypedInFilter) filter).getColumn();
    } else {
      return stringValuesColumn(((OrDimFilter) filter).getFields().get(0));
    }
  }

  private static Set<String> stringValues(final DimFilter filter)
  {
    if (filter instanceof SelectorDimFilter) {
      return Set.of(((SelectorDimFilter) filter).getValue());
    } else if (filter instanceof EqualityFilter) {
      return Set.of((String) ((EqualityFilter) filter).getMatchValue());
    } else if (filter instanceof InDimFilter) {
      return ((InDimFilter) filter).getValues();
    } else if (filter instanceof TypedInFilter) {
      return ((TypedInFilter) filter).getSortedValues()
                                           .stream()
                                           .map(String.class::cast)
                                           .collect(Collectors.toSet());
    } else {
      final Set<String> values = new HashSet<>();
      for (final DimFilter field : ((OrDimFilter) filter).getFields()) {
        values.addAll(stringValues(field));
      }
      return values;
    }
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
