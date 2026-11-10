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
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.query.NodeAttribute;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Descriptor for the native {@code sys.stack_trace} table.
 */
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

  private static final String SERVER_FILTER_REQUIRED_MESSAGE =
      "sys.stack_trace requires a filter on the server column using '=' or 'IN' that applies directly to its scan. "
      + "In joins and subqueries, filter sys.stack_trace in its own subquery";

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.values());
  private static final Map<String, NodeAttribute> NODE_COLUMNS = Map.of(
      "server", NodeAttribute.HOST_AND_PORT,
      "service_name", NodeAttribute.SERVICE_NAME
  );

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
    return SystemTableAuthorization.STATE_AND_CONFIG_READ_ROW_AUTHORIZER;
  }

  @Override
  public Map<String, NodeAttribute> getNodeColumns()
  {
    return NODE_COLUMNS;
  }

  @Override
  public boolean isEmptyDiscoveryAllowed()
  {
    return true;
  }

  /**
   * Rejects a SQL query that would take a thread dump on every service: the Scan sent to the services must filter on
   * {@code server} with {@code =} or {@code IN}, checked before any service is queried. A native query reaches only
   * the contacted service, so it doesn't go through this check. Also rejects an invalid
   * {@code maxStackTraceFrameDepth}.
   */
  @Override
  public void validateNodeQuery(final Query<?> nodeQuery)
  {
    InvalidInput.conditionalException(
        SystemTableFilters.getStringValues(
            Filters.toFilter(nodeQuery.getFilter()),
            StackTraceTableDataProvider.SERVER_COLUMN
        ) != null,
        SERVER_FILTER_REQUIRED_MESSAGE
    );

    // Parsing validates the range of the parameter.
    nodeQuery.context().get(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH);
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
            nodeRoles.stream().map(NodeRole::getJsonName).sorted().toList().toString(),
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
}
