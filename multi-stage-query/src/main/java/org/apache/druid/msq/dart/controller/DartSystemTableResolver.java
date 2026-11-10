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

package org.apache.druid.msq.dart.controller;

import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.query.SystemTableQueryClient;
import org.apache.druid.server.system.table.SystemTableAuthorization;
import org.apache.druid.server.system.table.SystemTableDescriptor;

import java.util.Map;

/**
 * Replaces the system tables of a Dart query with the rows that the caller may read, gathered from every service by
 * {@link SystemTableQueryClient}, as the Broker does for native SQL queries. The query then reads inline rows, which
 * Dart processes like any other inline datasource.
 */
public class DartSystemTableResolver
{
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final AuthorizerMapper authorizerMapper;
  private final Injector injector;

  @Inject
  public DartSystemTableResolver(
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final AuthorizerMapper authorizerMapper,
      final Injector injector
  )
  {
    this.tableDescriptors = tableDescriptors;
    this.authorizerMapper = authorizerMapper;
    this.injector = injector;
  }

  /**
   * Whether the system table can be read by Dart queries. Tables are registered only when system tables are enabled.
   */
  public boolean supports(final String tableName)
  {
    return tableDescriptors.containsKey(tableName);
  }

  /**
   * Gathers the rows of the query's system tables that the caller may read. The rows are gathered before this method
   * returns, and every node query has finished by then.
   *
   * @return the query with its system tables replaced by inline rows
   * @throws org.apache.druid.server.security.ForbiddenException if the caller may not read one of the tables at all
   */
  public <T> Query<T> resolve(final Query<T> query, final AuthenticationResult authenticationResult)
  {
    final DataSource authorized = SystemTableAuthorization.withAccess(
        SystemTableDataSource.withScope(query.getDataSource(), SystemTableDataSource.Scope.CLUSTER),
        tableDescriptors,
        authenticationResult,
        authorizerMapper
    );
    if (!onlyReadsSystemTables(authorized)) {
      // The rows of system tables are processed by the Dart worker embedded in the Broker, which can't read segments.
      throw InvalidInput.exception("Dart queries can't read system tables together with other datasources");
    }
    // SystemTableQueryClient is bound only when system tables are enabled, which is the only case in which Dart plans
    // a system table.
    final SystemTableQueryClient.ResolvedQuery<T> resolved =
        injector.getInstance(SystemTableQueryClient.class).resolve(query.withDataSource(authorized));
    try {
      return query.withDataSource(resolved.query().getDataSource());
    }
    finally {
      // The rows are already gathered, so this only releases the node queries' resources.
      try {
        resolved.closer().close();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Gathers the rows of the datasource's system tables that the caller may read, for a query with the given context.
   *
   * @return the datasource with its system tables replaced by inline rows
   */
  public DataSource resolve(
      final DataSource dataSource,
      final QueryContext queryContext,
      final AuthenticationResult authenticationResult
  )
  {
    if (!SystemTableDataSource.readsSystemTable(dataSource)) {
      return dataSource;
    }
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(dataSource)
                                  .eternityInterval()
                                  .context(queryContext.asMap())
                                  .build();
    return resolve(query, authenticationResult).getDataSource();
  }

  /**
   * Whether every leaf datasource that the datasource reads is a system table or inline rows.
   */
  static boolean onlyReadsSystemTables(final DataSource dataSource)
  {
    if (dataSource instanceof SystemTableDataSource || dataSource instanceof InlineDataSource) {
      return true;
    }
    if (dataSource.getChildren().isEmpty()) {
      return false;
    }
    for (final DataSource child : dataSource.getChildren()) {
      if (!onlyReadsSystemTables(child)) {
        return false;
      }
    }
    return true;
  }
}
