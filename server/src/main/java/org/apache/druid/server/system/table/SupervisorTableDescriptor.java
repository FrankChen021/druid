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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Descriptor for the native {@code sys.supervisors} table. */
public class SupervisorTableDescriptor implements SystemTableDescriptor
{
  public static final String TABLE_NAME = "supervisors";
  static final String AUTHORIZATION_DATASOURCES_COLUMN = "__system_table_authorization_datasources";
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
  private static final RowSignature TRANSPORT_ROW_SIGNATURE = RowSignature
      .builder()
      .addAll(ROW_SIGNATURE)
      .add(AUTHORIZATION_DATASOURCES_COLUMN, ColumnType.STRING_ARRAY)
      .build();

  private static final Set<NodeRole> NODE_ROLES = Set.of(NodeRole.OVERLORD);
  private static final int AUTHORIZATION_DATASOURCES_COLUMN_INDEX =
      TRANSPORT_ROW_SIGNATURE.indexOf(AUTHORIZATION_DATASOURCES_COLUMN);
  private static final SystemTableRowAuthorizer ROW_AUTHORIZER = new SystemTableRowAuthorizer()
  {
    @Override
    public Iterable<Object[]> filterAuthorizedRows(
        final Iterable<Object[]> rows,
        final AuthenticationResult authenticationResult,
        final AuthorizerMapper authorizerMapper
    )
    {
      return AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          rows,
          SupervisorTableDescriptor::datasourceReadActions,
          authorizerMapper
      );
    }
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
    return SystemTableRoutingMode.LEADER_ONLY;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return ROW_SIGNATURE;
  }

  @Override
  public RowSignature getTransportRowSignature()
  {
    return TRANSPORT_ROW_SIGNATURE;
  }

  @Override
  public Object[] toPublicRow(final Object[] transportRow)
  {
    return Arrays.copyOf(transportRow, ROW_SIGNATURE.size());
  }

  @Override
  public SystemTableRowAuthorizer getRowAuthorizer()
  {
    return ROW_AUTHORIZER;
  }

  private static Iterable<ResourceAction> datasourceReadActions(final Object[] row)
  {
    final Object datasources = row[AUTHORIZATION_DATASOURCES_COLUMN_INDEX];
    if (datasources == null) {
      return null;
    }

    final List<ResourceAction> resourceActions = new ArrayList<>();
    if (datasources instanceof Iterable<?> iterable) {
      for (final Object datasource : iterable) {
        resourceActions.add(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(String.class.cast(datasource)));
      }
    } else if (datasources instanceof Object[] array) {
      for (final Object datasource : array) {
        resourceActions.add(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(String.class.cast(datasource)));
      }
    } else {
      throw new ISE("Invalid supervisor authorization datasource value of type[%s]", datasources.getClass());
    }
    return resourceActions;
  }
}
