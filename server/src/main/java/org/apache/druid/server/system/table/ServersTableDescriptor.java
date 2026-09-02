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
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;
import java.util.Set;

/** Descriptor for the native {@code sys.servers} table. */
public class ServersTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "servers";
  public static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("server_type", ColumnType.STRING)
      .add("tier", ColumnType.STRING)
      .add("curr_size", ColumnType.LONG)
      .add("max_size", ColumnType.LONG)
      .add("storage_size", ColumnType.LONG)
      .add("is_leader", ColumnType.LONG)
      .add("start_time", ColumnType.STRING)
      .add("version", ColumnType.STRING)
      .add("build_revision", ColumnType.STRING)
      .add("labels", ColumnType.NESTED_DATA)
      .add("available_processors", ColumnType.LONG)
      .add("total_memory", ColumnType.LONG)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.BROKER);
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
  public SystemTableRoutingMode getRoutingMode()
  {
    return SystemTableRoutingMode.LOCAL;
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
