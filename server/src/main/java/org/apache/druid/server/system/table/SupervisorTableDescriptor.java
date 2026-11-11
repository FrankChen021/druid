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

/**
 * Descriptor for the native {@code sys.supervisors} table, served by the leader Overlord.
 */
public class SupervisorTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "supervisors";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("supervisor_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("detailed_state", ColumnType.STRING)
      .add("healthy", ColumnType.LONG)
      .add("type", ColumnType.STRING)
      .add("source", ColumnType.STRING)
      .add("suspended", ColumnType.LONG)
      .add("spec", ColumnType.STRING)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.OVERLORD);
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER =
      SystemTableAuthorization.dataSourceReadRowAuthorizer(ROW_SIGNATURE.indexOf("datasource"));

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
  public SystemTableTargetNodes getTargetNodes()
  {
    return SystemTableTargetNodes.LEADER_ONLY;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  /**
   * Shows each supervisor only to callers with {@code DATASOURCE READ} on its {@code datasource}, as the SQL
   * interpreter does. A supervisor without a datasource is hidden from every caller.
   */
  @Override
  public SystemTableRowAuthorizer getRowAuthorizer()
  {
    return ROW_AUTHORIZER;
  }

  /**
   * The row authorizer shows only the supervisors of datasources the caller can read.
   */
  @Override
  public boolean hasRowLevelAuthorization()
  {
    return true;
  }
}
