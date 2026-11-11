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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.TaskMaster;
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
import java.util.Objects;
import java.util.Set;

/**
 * Native row supplier for {@code sys.supervisors}, served by the leader Overlord.
 */
public class SupervisorsTableDataProvider implements SystemTableDataProvider
{
  private static final String SUPERVISOR_ID_COLUMN = "supervisor_id";
  private static final Set<String> PUSHDOWN_COLUMNS = Set.of(SUPERVISOR_ID_COLUMN);

  private final TaskMaster taskMaster;
  private final ObjectMapper objectMapper;

  @Inject
  public SupervisorsTableDataProvider(
      final TaskMaster taskMaster,
      @Json final ObjectMapper objectMapper
  )
  {
    this.taskMaster = taskMaster;
    this.objectMapper = objectMapper;
  }

  /**
   * Returns {@code supervisor_id}, whose filters let the Overlord build rows for only the matching supervisors.
   */
  @Override
  public Set<String> getPushdownColumns()
  {
    return PUSHDOWN_COLUMNS;
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters)
  {
    // TaskMaster has a supervisor manager only while this Overlord is the full leader.
    final Optional<SupervisorManager> optionalManager = taskMaster.getSupervisorManager();
    if (!optionalManager.isPresent()) {
      throw notLeader();
    }

    final SupervisorManager manager = optionalManager.get();
    final Set<String> supervisorIds = filterSupervisorIds(manager.getSupervisorIds(), filters);
    final List<Object[]> rows = new ArrayList<>(supervisorIds.size());
    for (final String supervisorId : supervisorIds) {
      final Optional<SupervisorSpec> spec = manager.getSupervisorSpec(supervisorId);
      if (!spec.isPresent()) {
        continue;
      }
      final Optional<SupervisorStateManager.State> state = manager.getSupervisorState(supervisorId);
      rows.add(toRow(SupervisorStatusMapper.toStatus(objectMapper, supervisorId, state, spec, false, true)));
    }
    // Losing leadership clears the supervisor manager while the rows are read, which would otherwise return a
    // partial or empty table.
    if (!taskMaster.isFullLeader()) {
      throw notLeader();
    }
    return rows;
  }

  private static DruidException notLeader()
  {
    return DruidException.forPersona(DruidException.Persona.OPERATOR)
                         .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                         .build("sys.supervisors must be read from the current Overlord leader");
  }

  /**
   * Returns the supervisor IDs that may match every filter on {@code supervisor_id} that matches a set of strings.
   * Other filters are left to the query, which applies its whole filter to the returned rows.
   */
  static Set<String> filterSupervisorIds(final Set<String> allSupervisorIds, final List<Filter> filters)
  {
    final Set<String> supervisorIds = new HashSet<>(allSupervisorIds);
    for (final Filter filter : filters) {
      final StringValues stringValues = toStringValues(filter);
      if (stringValues != null && SUPERVISOR_ID_COLUMN.equals(stringValues.column)) {
        supervisorIds.retainAll(stringValues.values);
      }
    }
    return supervisorIds;
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

  private static Object[] toRow(final SupervisorStatus supervisor)
  {
    return new Object[]{
        supervisor.getId(),
        supervisor.getDataSource(),
        supervisor.getState(),
        supervisor.getDetailedState(),
        supervisor.isHealthy() ? 1L : 0L,
        supervisor.getType(),
        supervisor.getSource(),
        supervisor.isSuspended() ? 1L : 0L,
        supervisor.getSpecString()
    };
  }
}
