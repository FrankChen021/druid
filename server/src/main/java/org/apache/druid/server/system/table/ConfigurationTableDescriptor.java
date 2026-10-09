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
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Describes the per-process configuration catalog supplied through the native system-table framework. */
public class ConfigurationTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "configuration";
  public static final RowSignature ROW_SIGNATURE = RowSignature.builder()
      .add("server", ColumnType.STRING)
      .add("service_name", ColumnType.STRING)
      .add("node_roles", ColumnType.STRING)
      .add("property", ColumnType.STRING)
      .add("config_class", ColumnType.STRING)
      .add("binding", ColumnType.STRING)
      .add("value_type", ColumnType.STRING)
      .add("configured_value", ColumnType.STRING)
      .add("effective_value", ColumnType.NESTED_DATA)
      .add("value_status", ColumnType.STRING)
      .add("error_message", ColumnType.STRING)
      .build();

  @Override
  public String getTableName()
  {
    return TABLE_NAME;
  }

  @Override
  public Set<NodeRole> getNodeRoles()
  {
    return Set.of(NodeRole.values());
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  @Override
  public SystemTableRowAuthorizer getRowAuthorizer()
  {
    return (rows, authenticationResult, authorizerMapper) -> {
      authorize(authenticationResult, authorizerMapper);
      return rows;
    };
  }

  /** Checks both the original SQL user and, at the provider, the internal native-query caller. */
  public static void authorize(final AuthenticationResult authenticationResult, final AuthorizerMapper authorizerMapper)
  {
    final AuthorizationResult result = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        List.of(new ResourceAction(new Resource("CONFIG", ResourceType.CONFIG), Action.READ)),
        authorizerMapper
    );
    if (!result.allowAccessWithNoRestriction()) {
      throw new ForbiddenException("Insufficient permission to read configuration");
    }
  }

  @Override
  public Optional<Object[]> getNodeFailureRow(
      final DruidNode node,
      final Set<NodeRole> nodeRoles,
      final Exception failure
  )
  {
    // Exception messages can contain configuration values. Do not expose them through the table.
    return Optional.of(new Object[]{
        node.getHostAndPortToUse(), node.getServiceName(),
        nodeRoles.stream().map(NodeRole::getJsonName).sorted().toList().toString(),
        null, null, null, null, null, null, "ERROR", "Unable to read node configuration"
    });
  }
}
