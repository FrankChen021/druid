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

import org.apache.druid.query.DataSource;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Authorization checks shared by native system tables.
 */
public final class SystemTableAuthorization
{
  /**
   * Row authorizer for tables whose rows are all visible to users with {@code STATE READ}, and to no one else.
   */
  public static final SystemTableRowAuthorizer STATE_READ_ROW_AUTHORIZER =
      (authenticationResult, authorizerMapper) -> {
        authorizeStateRead(authenticationResult, authorizerMapper);
        return SystemTableAccess.ALL_ROWS;
      };

  private SystemTableAuthorization()
  {
  }

  /**
   * Attaches to each system table in the datasource the {@link SystemTableAccess} of the given identity, which says
   * which rows of the table it may read. The table can't be read without it. Tables without a descriptor are left as
   * they are.
   *
   * @throws ForbiddenException if the identity may not read one of the tables at all
   */
  public static DataSource withAccess(
      final DataSource dataSource,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final AuthenticationResult authenticationResult,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (!SystemTableDataSource.readsSystemTable(dataSource)) {
      return dataSource;
    }
    if (dataSource instanceof SystemTableDataSource systemTable) {
      final SystemTableDescriptor descriptor = tableDescriptors.get(systemTable.getTable());
      if (descriptor == null) {
        return systemTable;
      }
      return systemTable.withAccess(descriptor.getRowAuthorizer().authorize(authenticationResult, authorizerMapper));
    }
    final List<DataSource> children = dataSource.getChildren();
    final List<DataSource> authorizedChildren = new ArrayList<>(children.size());
    for (final DataSource child : children) {
      authorizedChildren.add(withAccess(child, tableDescriptors, authenticationResult, authorizerMapper));
    }
    return dataSource.withChildren(authorizedChildren);
  }

  /**
   * Requires {@code STATE READ}, the permission the SQL system schema requires to view servers.
   *
   * @throws ForbiddenException if the identity lacks the permission
   */
  public static void authorizeStateRead(
      final AuthenticationResult authenticationResult,
      final AuthorizerMapper authorizerMapper
  )
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException("Insufficient permission to view servers: " + authorizationResult.getErrorMessage());
    }
  }
}
