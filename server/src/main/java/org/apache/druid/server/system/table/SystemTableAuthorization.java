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

import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
   * Whether a filter only compares raw column values with literals, so evaluating it on a row can't fail or have
   * effects that depend on the row. Such a filter can run before row authorization without revealing hidden rows.
   */
  public static boolean isRowSafeFilter(final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> selector.getExtractionFn() == null;
      case InDimFilter in -> in.getExtractionFn() == null;
      case BoundDimFilter bound -> bound.getExtractionFn() == null;
      case LikeDimFilter like -> like.getExtractionFn() == null;
      case EqualityFilter ignored -> true;
      case TypedInFilter ignored -> true;
      case NullFilter ignored -> true;
      case RangeFilter ignored -> true;
      case AndDimFilter and -> and.getFields().stream().allMatch(SystemTableAuthorization::isRowSafeFilter);
      case OrDimFilter or -> or.getFields().stream().allMatch(SystemTableAuthorization::isRowSafeFilter);
      case NotDimFilter not -> isRowSafeFilter(not.getField());
      default -> false;
    };
  }

  /**
   * Returns a row authorizer that shows each row only to callers with {@code DATASOURCE READ} on the datasource in
   * the given column, as the SQL system schema does for tables such as {@code sys.tasks}. A row without a datasource
   * has no resource to authorize, so it is hidden from every caller.
   */
  public static SystemTableRowAuthorizer dataSourceReadRowAuthorizer(final int dataSourceColumn)
  {
    return (authenticationResult, authorizerMapper) -> {
      // Rows of one datasource repeat, so authorize each datasource once per query.
      final Map<String, Boolean> readableDataSources = new ConcurrentHashMap<>();
      return new SystemTableAccess(
          row -> {
            final String dataSource = (String) row[dataSourceColumn];
            return dataSource != null && readableDataSources.computeIfAbsent(
                dataSource,
                name -> AuthorizationUtils.authorizeAllResourceActions(
                    authenticationResult,
                    Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(name)),
                    authorizerMapper
                ).allowBasicAccess()
            );
          }
      );
    };
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
