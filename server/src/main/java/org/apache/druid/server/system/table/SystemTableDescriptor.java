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

import java.util.Optional;
import java.util.Set;

/**
 * Describes a native system table and how its rows are distributed across Druid services.
 */
public interface SystemTableDescriptor
{
  String getTableName();

  /**
   * Returns the node roles of the services that serve this table. Every discovered service with one of these roles
   * contributes its own rows.
   */
  Set<NodeRole> getNodeRoles();

  RowSignature getRowSignature();

  SystemTableRowAuthorizer getRowAuthorizer();

  /**
   * Whether the row authorizer can hide some rows from a caller while showing others. The services the Broker reads
   * the table from use the Broker's escalated identity, and the Broker applies the caller's access only to the gathered
   * rows. For such a table, the Broker keeps the node scans from revealing hidden rows: it pushes only filters that
   * can't fail on a row's values, and it doesn't let the caller lower the scans' scatter-gather limit, whose error
   * would report the bytes of hidden rows.
   */
  default boolean hidesRows()
  {
    return false;
  }

  /**
   * Returns which services' rows SQL reads for this table.
   */
  default SystemTableRoutingMode getRoutingMode()
  {
    return SystemTableRoutingMode.ALL_NODES;
  }

  /**
   * Whether an empty discovery result represents an empty table instead of unavailable infrastructure.
   */
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
