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

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;
import java.util.List;

/** Authorization checks shared by native system tables. */
public final class SystemTableAuthorization
{
  /** The permission that {@link #authorizeStateRead} requires. */
  public static final ResourceAction STATE_READ = new ResourceAction(Resource.STATE_RESOURCE, Action.READ);

  /** Row authorizer for tables whose rows are all visible to users with {@code STATE READ}, and to no one else. */
  public static final SystemTableRowAuthorizer STATE_READ_ROW_AUTHORIZER =
      (rows, authenticationResult, authorizerMapper) -> {
        authorizeStateRead(authenticationResult, authorizerMapper);
        return rows;
      };

  private SystemTableAuthorization()
  {
  }

  /**
   * Reads a table's rows from its provider and applies the table's row authorizer for the given identity. Rows are
   * read lazily, when the result is iterated.
   */
  public static Iterable<Object[]> readAuthorizedRows(
      final SystemTableDescriptor descriptor,
      final SystemTableDataProvider provider,
      final List<DimFilter> pushdownFilters,
      final AuthenticationResult authenticationResult,
      final AuthorizerMapper authorizerMapper
  )
  {
    final Iterable<Object[]> rows = () -> provider.getRows(pushdownFilters).iterator();
    return descriptor.getRowAuthorizer().filterAuthorizedRows(rows, authenticationResult, authorizerMapper);
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
        Collections.singletonList(STATE_READ),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException("Insufficient permission to view servers: " + authorizationResult.getErrorMessage());
    }
  }
}
