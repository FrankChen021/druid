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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskState;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Storage-level prefilters for task status queries. These filters must only reduce the result to a superset of the
 * rows accepted by the original query because the native filter remains the final correctness filter.
 */
public class TaskStorageQueryFilter
{
  public enum Column
  {
    TASK_ID,
    GROUP_ID,
    TYPE,
    DATASOURCE,
    CREATED_TIME,
    STATUS
  }

  @Nullable
  private final Set<String> taskIds;
  @Nullable
  private final Set<String> groupIds;
  @Nullable
  private final Set<String> types;
  @Nullable
  private final Set<String> dataSources;
  @Nullable
  private final Set<String> createdTimes;
  @Nullable
  private final String createdTimeLower;
  private final boolean createdTimeLowerOpen;
  @Nullable
  private final String createdTimeUpper;
  private final boolean createdTimeUpperOpen;
  @Nullable
  private final Boolean active;
  private final boolean matchesNothing;

  private TaskStorageQueryFilter(
      @Nullable final Set<String> taskIds,
      @Nullable final Set<String> groupIds,
      @Nullable final Set<String> types,
      @Nullable final Set<String> dataSources,
      @Nullable final Set<String> createdTimes,
      @Nullable final String createdTimeLower,
      final boolean createdTimeLowerOpen,
      @Nullable final String createdTimeUpper,
      final boolean createdTimeUpperOpen,
      @Nullable final Boolean active,
      final boolean matchesNothing
  )
  {
    this.taskIds = immutableCopy(taskIds);
    this.groupIds = immutableCopy(groupIds);
    this.types = immutableCopy(types);
    this.dataSources = immutableCopy(dataSources);
    this.createdTimes = immutableCopy(createdTimes);
    this.createdTimeLower = createdTimeLower;
    this.createdTimeLowerOpen = createdTimeLowerOpen;
    this.createdTimeUpper = createdTimeUpper;
    this.createdTimeUpperOpen = createdTimeUpperOpen;
    this.active = active;
    this.matchesNothing = matchesNothing;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static TaskStorageQueryFilter forDataSource(@Nullable final String dataSource)
  {
    final Builder builder = builder();
    if (dataSource != null) {
      builder.addValues(Column.DATASOURCE, Set.of(dataSource));
    }
    return builder.build();
  }

  @Nullable
  public Set<String> getTaskIds()
  {
    return taskIds;
  }

  @Nullable
  public Set<String> getGroupIds()
  {
    return groupIds;
  }

  @Nullable
  public Set<String> getTypes()
  {
    return types;
  }

  @Nullable
  public Set<String> getDataSources()
  {
    return dataSources;
  }

  @Nullable
  public Set<String> getCreatedTimes()
  {
    return createdTimes;
  }

  @Nullable
  public String getCreatedTimeLower()
  {
    return createdTimeLower;
  }

  public boolean isCreatedTimeLowerOpen()
  {
    return createdTimeLowerOpen;
  }

  @Nullable
  public String getCreatedTimeUpper()
  {
    return createdTimeUpper;
  }

  public boolean isCreatedTimeUpperOpen()
  {
    return createdTimeUpperOpen;
  }

  @Nullable
  public Boolean getActive()
  {
    return active;
  }

  public boolean includesActiveTasks()
  {
    return !matchesNothing && !Boolean.FALSE.equals(active);
  }

  public boolean includesCompleteTasks()
  {
    return !matchesNothing && !Boolean.TRUE.equals(active);
  }

  public boolean matchesNothing()
  {
    return matchesNothing;
  }

  @Nullable
  public String getSingleDataSource()
  {
    return singleValue(dataSources);
  }

  @Nullable
  public String getSingleType()
  {
    return singleValue(types);
  }

  @Nullable
  private static Set<String> immutableCopy(@Nullable final Set<String> values)
  {
    return values == null ? null : ImmutableSet.copyOf(values);
  }

  @Nullable
  private static String singleValue(@Nullable final Set<String> values)
  {
    return values != null && values.size() == 1 ? values.iterator().next() : null;
  }

  public static class Builder
  {
    @Nullable
    private Set<String> taskIds;
    @Nullable
    private Set<String> groupIds;
    @Nullable
    private Set<String> types;
    @Nullable
    private Set<String> dataSources;
    @Nullable
    private Set<String> createdTimes;
    @Nullable
    private Set<String> statuses;
    @Nullable
    private String createdTimeLower;
    private boolean createdTimeLowerOpen;
    @Nullable
    private String createdTimeUpper;
    private boolean createdTimeUpperOpen;
    private boolean matchesNothing;

    public Builder addValues(final Column column, final Collection<String> values)
    {
      final Set<String> newValues = ImmutableSet.copyOf(values);
      if (newValues.isEmpty()) {
        matchesNothing = true;
        return this;
      }

      switch (column) {
        case TASK_ID:
          taskIds = intersect(taskIds, newValues);
          matchesNothing = matchesNothing || taskIds.isEmpty();
          break;
        case GROUP_ID:
          groupIds = intersect(groupIds, newValues);
          matchesNothing = matchesNothing || groupIds.isEmpty();
          break;
        case TYPE:
          types = intersect(types, newValues);
          matchesNothing = matchesNothing || types.isEmpty();
          break;
        case DATASOURCE:
          dataSources = intersect(dataSources, newValues);
          matchesNothing = matchesNothing || dataSources.isEmpty();
          break;
        case CREATED_TIME:
          createdTimes = intersect(createdTimes, newValues);
          matchesNothing = matchesNothing || createdTimes.isEmpty();
          break;
        case STATUS:
          statuses = intersect(statuses, newValues);
          matchesNothing = matchesNothing || statuses.isEmpty();
          break;
        default:
          throw new IllegalStateException("Unsupported task filter column");
      }
      return this;
    }

    public Builder addCreatedTimeRange(
        @Nullable final String lower,
        final boolean lowerOpen,
        @Nullable final String upper,
        final boolean upperOpen
    )
    {
      if (lower != null
          && (createdTimeLower == null
              || lower.compareTo(createdTimeLower) > 0
              || lower.equals(createdTimeLower) && lowerOpen)) {
        createdTimeLower = lower;
        createdTimeLowerOpen = lowerOpen;
      }
      if (upper != null
          && (createdTimeUpper == null
              || upper.compareTo(createdTimeUpper) < 0
              || upper.equals(createdTimeUpper) && upperOpen)) {
        createdTimeUpper = upper;
        createdTimeUpperOpen = upperOpen;
      }
      return this;
    }

    public TaskStorageQueryFilter build()
    {
      if (createdTimeLower != null && createdTimeUpper != null) {
        final int comparison = createdTimeLower.compareTo(createdTimeUpper);
        matchesNothing |= comparison > 0
                          || comparison == 0 && (createdTimeLowerOpen || createdTimeUpperOpen);
      }

      final Boolean active = activeFromStatuses();
      return new TaskStorageQueryFilter(
          taskIds,
          groupIds,
          types,
          dataSources,
          createdTimes,
          createdTimeLower,
          createdTimeLowerOpen,
          createdTimeUpper,
          createdTimeUpperOpen,
          active,
          matchesNothing
      );
    }

    @Nullable
    private Boolean activeFromStatuses()
    {
      if (statuses == null) {
        return null;
      }

      final boolean includesRunning = statuses.contains(TaskState.RUNNING.name());
      final boolean includesComplete = statuses.contains(TaskState.SUCCESS.name())
                                       || statuses.contains(TaskState.FAILED.name());
      if (!includesRunning && !includesComplete) {
        matchesNothing = true;
        return null;
      } else if (includesRunning && includesComplete) {
        return null;
      } else {
        return includesRunning;
      }
    }

    private static Set<String> intersect(
        @Nullable final Set<String> currentValues,
        final Set<String> newValues
    )
    {
      if (currentValues == null) {
        return new HashSet<>(newValues);
      }
      final Set<String> intersection = new HashSet<>(currentValues);
      intersection.retainAll(newValues);
      return intersection;
    }
  }
}
