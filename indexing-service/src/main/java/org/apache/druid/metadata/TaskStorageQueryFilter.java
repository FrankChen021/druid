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

package org.apache.druid.metadata;

import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable wrapper for the validated native filters pushed into task metadata queries.
 */
public final class TaskStorageQueryFilter
{
  private final List<DimFilter> filters;
  private final boolean includesActiveTasks;
  private final boolean includesCompleteTasks;

  public TaskStorageQueryFilter(final List<DimFilter> filters)
  {
    this.filters = List.copyOf(filters);
    final Set<Boolean> possibleTaskActiveness = computePossibleTaskActiveness();
    this.includesActiveTasks = possibleTaskActiveness.contains(true);
    this.includesCompleteTasks = possibleTaskActiveness.contains(false);
  }

  public List<DimFilter> getFilters()
  {
    return filters;
  }

  /**
   * Returns the column of a string-values filter: a selector, equality, IN, or an OR of those on one column.
   */
  public static String getStringValuesColumn(final DimFilter filter)
  {
    if (!isStringValuesFilter(filter)) {
      throw DruidException.defensive("Filter[%s] isn't a string-values filter", filter);
    }
    return switch (filter) {
      case SelectorDimFilter selector -> selector.getDimension();
      case EqualityFilter equality -> equality.getColumn();
      case InDimFilter in -> in.getDimension();
      case TypedInFilter in -> in.getColumn();
      case OrDimFilter or -> getStringValuesColumn(or.getFields().get(0));
      case null, default -> throw DruidException.defensive("Filter[%s] isn't a string-values filter", filter);
    };
  }

  /**
   * Returns the values matched by a string-values filter.
   */
  public static Set<String> getStringValues(final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> Set.of(selector.getValue());
      case EqualityFilter equality -> Set.of((String) equality.getMatchValue());
      case InDimFilter in -> in.getValues();
      case TypedInFilter in -> in.getSortedValues().stream().map(String.class::cast).collect(Collectors.toSet());
      case OrDimFilter or -> {
        final Set<String> values = new HashSet<>();
        for (final DimFilter field : or.getFields()) {
          values.addAll(getStringValues(field));
        }
        yield values;
      }
      case null, default -> throw DruidException.defensive("Filter[%s] isn't a string-values filter", filter);
    };
  }

  TaskStorageQueryFilter withoutStatusFilters()
  {
    return new TaskStorageQueryFilter(
        filters.stream().filter(filter -> !"status".equals(getColumn(filter))).toList()
    );
  }

  public boolean includesActiveTasks()
  {
    return includesActiveTasks;
  }

  public boolean includesCompleteTasks()
  {
    return includesCompleteTasks;
  }

  private Set<Boolean> getPossibleTaskActiveness(final DimFilter statusFilter)
  {
    final Set<Boolean> possibleActiveness = new HashSet<>();
    for (final String status : getStringValues(statusFilter)) {
      if (TaskState.RUNNING.name().equals(status)) {
        possibleActiveness.add(true);
      } else if (TaskState.SUCCESS.name().equals(status) || TaskState.FAILED.name().equals(status)) {
        possibleActiveness.add(false);
      }
    }
    return possibleActiveness;
  }

  /**
   * Whether a filter matches non-null string values of one column: a selector, a string equality, an IN, a string
   * typed IN, or a non-empty OR of those on a single column.
   */
  private static boolean isStringValuesFilter(@Nullable final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> selector.getValue() != null && selector.getExtractionFn() == null;
      case EqualityFilter equality -> ColumnType.STRING.equals(equality.getMatchValueType())
                                      && equality.getMatchValue() instanceof String;
      case InDimFilter in -> in.getExtractionFn() == null && in.getValues().stream().allMatch(Objects::nonNull);
      case TypedInFilter in -> ColumnType.STRING.equals(in.getMatchValueType())
                               && in.getSortedValues().stream().allMatch(String.class::isInstance);
      case OrDimFilter or -> !or.getFields().isEmpty()
                             && or.getFields().stream().allMatch(TaskStorageQueryFilter::isStringValuesFilter)
                             && or.getFields()
                                  .stream()
                                  .map(TaskStorageQueryFilter::getStringValuesColumn)
                                  .distinct()
                                  .count() == 1;
      case null, default -> false;
    };
  }

  private Set<Boolean> computePossibleTaskActiveness()
  {
    final Set<Boolean> possibleActiveness = new HashSet<>(Set.of(true, false));
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter) && "status".equals(getStringValuesColumn(filter))) {
        possibleActiveness.retainAll(getPossibleTaskActiveness(filter));
      }
    }
    return possibleActiveness;
  }

  @Nullable
  private String getColumn(final DimFilter filter)
  {
    return switch (filter) {
      case BoundDimFilter bound -> bound.getDimension();
      case RangeFilter range -> range.getColumn();
      case LikeDimFilter like -> like.getDimension();
      case NotDimFilter not -> getColumn(not.getField());
      default -> isStringValuesFilter(filter) ? getStringValuesColumn(filter) : null;
    };
  }
}
