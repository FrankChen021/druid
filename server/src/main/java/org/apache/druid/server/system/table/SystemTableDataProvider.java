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

import org.apache.druid.query.filter.Filter;

import javax.annotation.Nullable;
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
   * Returns the table's rows, in row-signature order, each time it's called. A {@code STRING} column holds a single
   * {@link String} or null, and a {@code LONG} column holds a {@link Number} or null. It's called synchronously on
   * the thread that runs the query.
   *
   * @param filters filters that read only {@link #getPushdownColumns() pushdown columns}. The result must include
   *                every row matching all of them, and may include other rows: the query applies its whole filter to
   *                the result.
   */
  Iterable<Object[]> getRows(List<Filter> filters);

  /**
   * Like {@link #getRows(List)}, for a query that reads only some columns. The default implementation ignores the
   * columns and returns full rows.
   *
   * <p>A provider can leave the other columns null to skip computing them. It must still fill every column that the
   * table's {@link SystemTableRowAuthorizer} reads, since rows are authorized after this method returns. If the
   * iterator of the returned rows is {@link java.io.Closeable}, it's closed once the query stops reading it, even if
   * the query stops early.</p>
   *
   * @param columns columns the query reads, or null if it may read every column
   */
  default Iterable<Object[]> getRows(final List<Filter> filters, @Nullable final Set<String> columns)
  {
    return getRows(filters);
  }
}
