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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.ResourceAction;

import java.util.Optional;
import java.util.Set;

/** Describes a system-table datasource and how its rows are distributed across Druid services. */
public interface SystemTableDescriptor
{
  String getTableName();

  /**
   * Returns the node roles capable of serving this table. {@link #getRoutingMode()} determines whether every node or
   * only the leader for the role receives a query.
   */
  Set<NodeRole> getNodeRoles();

  default SystemTableRoutingMode getRoutingMode()
  {
    return SystemTableRoutingMode.ALL_NODES;
  }

  RowSignature getRowSignature();

  SystemTableRowAuthorizer getRowAuthorizer();

  /**
   * Returns the resource actions that a caller needs in order to query this table. Engines that execute the table as
   * a datasource require them when the SQL statement is authorized, before any work is dispatched, because the
   * nodes that serve the rows only know the internal identity of the requesting service, not the original caller.
   */
  Set<ResourceAction> getResourceActions();

  /** Whether an empty discovery result represents an empty table instead of unavailable infrastructure. */
  default boolean isEmptyDiscoveryAllowed()
  {
    return false;
  }

  /**
   * Converts a node failure into a table row when the table supports partial results. An empty result means the
   * node failure must fail the query.
   */
  default Optional<Object[]> getNodeFailureRow(
      final DruidNode node,
      final Set<NodeRole> nodeRoles,
      final Exception failure
  )
  {
    return Optional.empty();
  }
}
