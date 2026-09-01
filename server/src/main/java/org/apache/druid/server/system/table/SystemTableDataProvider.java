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

import jakarta.validation.constraints.NotNull;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Supplies storage-prefiltered rows authorized for the internal caller of one native system table.
 * The implementation is deployed in related service.
 * For example, the data provider of sys.tasks is deployed in overlord module
 * */
public interface SystemTableDataProvider
{
  default List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return Collections.emptyList();
  }

  /**
   * Returns a query-local datasource over the framework-authorized raw rows, or empty to use the inline-row fallback.
   * Implementations must derive the returned datasource solely from {@code authorizedRows}; authorization and storage
   * filter pushdown have already been applied by the framework.
   */
  default Optional<DataSource> getAuthorizedDataSource(
      final SystemTableQueryRequest request,
      final Iterable<Object[]> authorizedRows
  )
  {
    return Optional.empty();
  }

  Iterable<Object[]> getRows(
      @NotNull List<DimFilter> filters,
      AuthenticationResult internalAuthenticationResult
  );

  /**
   * Returns full-width rows before any type conversion that can be deferred until column projection. Providers whose
   * rows already match the descriptor signature can use the default implementation.
   */
  default Iterable<Object[]> getRawRows(
      @NotNull final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    return getRows(filters, internalAuthenticationResult);
  }

  /** Projects one authorized raw row into the columns requested by the native query. */
  default Object[] projectRow(final Object[] row, @Nullable final int[] projects)
  {
    if (projects == null) {
      return row;
    }

    final Object[] projectedRow = new Object[projects.length];
    for (int i = 0; i < projects.length; i++) {
      projectedRow[i] = row[projects[i]];
    }
    return projectedRow;
  }
}
