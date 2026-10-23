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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Set;

/** Descriptor for the native {@code sys.queries} table. */
public class QueriesTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "queries";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("id", ColumnType.STRING)
      .add("engine", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("info", ColumnType.STRING)
      .add("initial_query_id", ColumnType.STRING)
      .add("is_initial_query", ColumnType.LONG)
      .add("server", ColumnType.STRING)
      .add("server_type", ColumnType.STRING)
      .build();

  /**
   * Native rows don't record who ran each query, so every row needs {@code STATE READ}. This is stricter than the SQL
   * {@code sys.queries} table, which also shows users without it their own Dart queries.
   */
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER =
      SystemTableAuthorization.stateReadRowAuthorizer("Insufficient permission to view queries");

  private static final Set<NodeRole> NODE_ROLES = Set.of(
      NodeRole.BROKER,
      NodeRole.HISTORICAL,
      NodeRole.OVERLORD,
      NodeRole.PEON,
      NodeRole.INDEXER
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
    return ROW_AUTHORIZER;
  }

  @Override
  public boolean isEmptyDiscoveryAllowed()
  {
    return true;
  }
}
