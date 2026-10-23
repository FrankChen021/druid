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

import org.apache.druid.query.QueryContext;
import org.apache.druid.query.filter.Filter;

import java.util.List;
import java.util.Set;

/**
 * Supplies this server's rows of one system table. Authorization isn't applied here: the caller's
 * {@link org.apache.druid.query.SystemTableAccess} filters the rows wherever they are read.
 *
 * <p>The provider is bound on the services that serve the table. For example, the provider of
 * {@code sys.server_properties} is bound on every service.</p>
 */
public interface SystemTableDataProvider
{
  /**
   * Columns whose filters {@link #getRows} can use to read fewer rows from storage.
   */
  default Set<String> getPushdownColumns()
  {
    return Set.of();
  }

  /**
   * Returns the table's rows, in row-signature order, each time it's called. It's called synchronously on the thread
   * that runs the query.
   *
   * @param filters filters that read only {@link #getPushdownColumns() pushdown columns}. The result must include
   *                every row matching all of them, and may include other rows: the query applies its whole filter to
   *                the result.
   */
  Iterable<Object[]> getRows(List<Filter> filters);

  /**
   * Returns the table's rows like {@link #getRows(List)}, for a query with the given context. Providers with options
   * that the query sets through its context override this method; the default ignores the context.
   *
   * @param filters      filters that read only {@link #getPushdownColumns() pushdown columns}, as for
   *                     {@link #getRows(List)}
   * @param queryContext context of the query that reads the table
   */
  default Iterable<Object[]> getRows(final List<Filter> filters, final QueryContext queryContext)
  {
    return getRows(filters);
  }
}
