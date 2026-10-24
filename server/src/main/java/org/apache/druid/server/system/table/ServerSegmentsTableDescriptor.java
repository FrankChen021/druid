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

/** Descriptor for the native {@code sys.server_segments} table. */
public class ServerSegmentsTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "server_segments";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("segment_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.BROKER);
  private static final SystemTableRowAuthorizer DATASOURCE_READ_ROW_AUTHORIZER =
      SystemTableAuthorization.dataSourceReadRowAuthorizer(ROW_SIGNATURE.indexOf("datasource"));

  /**
   * Same as SQL {@code sys.server_segments}: the table requires {@code STATE READ}, and each row requires
   * {@code DATASOURCE READ} on the segment's datasource.
   */
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER = (rows, authenticationResult, authorizerMapper) -> {
    SystemTableAuthorization.authorizeStateRead(authenticationResult, authorizerMapper);
    return DATASOURCE_READ_ROW_AUTHORIZER.filterAuthorizedRows(rows, authenticationResult, authorizerMapper);
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
  public SystemTableRoutingMode getRoutingMode()
  {
    return SystemTableRoutingMode.LOCAL_ONLY;
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
}
