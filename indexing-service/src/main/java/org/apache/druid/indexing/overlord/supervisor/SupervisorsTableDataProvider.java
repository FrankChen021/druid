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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Native row supplier for {@code sys.supervisors}. */
public class SupervisorsTableDataProvider implements SystemTableDataProvider
{
  private static final String SUPERVISOR_ID_COLUMN = "supervisor_id";
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter(SUPERVISOR_ID_COLUMN, null)
  );

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

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    final Optional<SupervisorManager> optionalManager = taskMaster.getSupervisorManager();
    if (!optionalManager.isPresent()) {
      throw new SystemTableNotLeaderException("overlord");
    }

    final SupervisorManager manager = optionalManager.get();
    final Set<String> supervisorIds = filterSupervisorIds(manager.getSupervisorIds(), filters);
    final List<Object[]> rows = new ArrayList<>(supervisorIds.size());
    for (final String supervisorId : supervisorIds) {
      rows.add(toRow(SupervisorStatusMapper.toStatus(objectMapper, manager, supervisorId, false, true)));
    }
    return rows;
  }

  private static Set<String> filterSupervisorIds(
      final Set<String> allSupervisorIds,
      final List<DimFilter> filters
  )
  {
    final Set<String> supervisorIds = new HashSet<>(allSupervisorIds);
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter)
          && SUPERVISOR_ID_COLUMN.equals(SystemTablePushdownFilter.getStringValuesColumn(filter))) {
        supervisorIds.retainAll(SystemTablePushdownFilter.getStringValues(filter));
      }
    }
    return supervisorIds;
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
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
