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
import org.apache.druid.query.Query;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;

import java.util.Map;
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
   * Validates the Scan that the Broker sends to every service to gather the table's rows, before it sends it. A table
   * can reject a query here that would be too costly to run on every service.
   *
   * @throws org.apache.druid.error.DruidException if the query is invalid
   */
  default void validateNodeQuery(final Query<?> nodeQuery)
  {
  }

  /**
   * Returns the columns of the table whose value on every row is an attribute of the service that returned the row,
   * keyed by column name. The Broker queries only the services whose attributes the node Scan's filter allows, by
   * reading {@code =}, {@code IN}, and {@code OR}-of-{@code =} filters on these columns with
   * {@link SystemTableFilters}. The services still apply the whole filter, so this doesn't change the rows returned,
   * except that a service the filter rules out can no longer fail the query. By default no column is declared, and
   * every service with one of the table's {@link #getNodeRoles() roles} is queried.
   */
  default Map<String, NodeAttribute> getNodeColumns()
  {
    return Map.of();
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
