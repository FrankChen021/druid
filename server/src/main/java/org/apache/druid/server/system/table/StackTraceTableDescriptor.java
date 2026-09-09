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

package org.apache.druid.server.system.table;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Descriptor for the native {@code sys.stack_trace} table. */
public class StackTraceTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "stack_trace";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("service_name", ColumnType.STRING)
      .add("node_roles", ColumnType.STRING)
      .add("collected_at", ColumnType.STRING)
      .add("thread_id", ColumnType.LONG)
      .add("thread_name", ColumnType.STRING)
      .add("thread_state", ColumnType.STRING)
      .add("daemon", ColumnType.LONG)
      .add("priority", ColumnType.LONG)
      .add("cpu_time_ns", ColumnType.LONG)
      .add("user_cpu_time_ns", ColumnType.LONG)
      .add("lock_name", ColumnType.STRING)
      .add("lock_owner_id", ColumnType.LONG)
      .add("lock_owner_name", ColumnType.STRING)
      .add("is_deadlocked", ColumnType.LONG)
      .add("stack", ColumnType.STRING)
      .add("error_message", ColumnType.STRING)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.values());
  private static final List<SystemTablePushdownFilter> SERVER_FILTER = List.of(
      new SystemTablePushdownFilter("server", null)
  );
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER = (rows, authenticationResult, authorizerMapper) -> {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authorizationResult.getErrorMessage());
    }
    return rows;
  };

  @Override
  public String getTableName()
  {
    return TABLE_NAME;
  }

  @Override
  public Set<NodeRole> getNodeRoles()
  {
    return NODE_ROLES;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  @Override
  public SystemTableRowAuthorizer getRowAuthorizer()
  {
    return ROW_AUTHORIZER;
  }

  @Override
  public boolean isEmptyDiscoveryAllowed()
  {
    return true;
  }

  @Override
  public void validateQuery(final Query<?> query)
  {
    final boolean hasServerFilter = SystemTablePushdownFilter.extract(query, SERVER_FILTER)
                                                       .stream()
                                                       .anyMatch(
                                                           filter -> isServerStringValuesFilter(filter)
                                                       );
    InvalidInput.conditionalException(
        hasServerFilter,
        "sys.stack_trace requires a filter on the server column using '=' or 'IN'"
    );
  }

  @Override
  public Optional<Object[]> getNodeFailureRow(
      final DruidNode node,
      final Set<NodeRole> nodeRoles,
      final Exception failure
  )
  {
    final String errorMessage = failure.getMessage() == null
                                ? failure.getClass().getSimpleName()
                                : failure.getMessage();
    return Optional.of(
        new Object[]{
            node.getHostAndPortToUse(),
            node.getServiceName(),
            nodeRoles.stream().map(NodeRole::getJsonName).sorted().collect(Collectors.joining(",")),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            errorMessage
        }
    );
  }

  private static boolean isServerStringValuesFilter(final DimFilter filter)
  {
    return (filter instanceof SelectorDimFilter
            || filter instanceof EqualityFilter
            || filter instanceof InDimFilter
            || filter instanceof TypedInFilter
            || filter instanceof OrDimFilter)
           && "server".equals(SystemTablePushdownFilter.getStringValuesColumn(filter));
  }
}
