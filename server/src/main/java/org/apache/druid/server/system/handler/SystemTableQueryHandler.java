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

package org.apache.druid.server.system.handler;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.SystemTableQueryRequest;

import java.util.List;
import java.util.Map;

/** Resolves one node-local system table either to inline rows or through the standard native Scan stack. */
public class SystemTableQueryHandler implements DataSourceQueryHandler
{
  private final Map<String, SystemTableDataProvider> dataSuppliers;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final ScanQueryEngine scanQueryEngine;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SystemTableQueryHandler(
      final Map<String, SystemTableDataProvider> dataSuppliers,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final ScanQueryEngine scanQueryEngine,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.dataSuppliers = dataSuppliers;
    this.tableDescriptors = tableDescriptors;
    this.scanQueryEngine = scanQueryEngine;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult requestAuthenticationResult,
      final boolean executeLocally
  )
  {
    if (!(query instanceof ScanQuery)) {
      throw new IAE("Local system table queries must be scan queries");
    }

    final SystemTableDataSource dataSource = (SystemTableDataSource) query.getDataSource();
    final SystemTableDataProvider dataSupplier = getDataSupplier(dataSource);
    final SystemTableDescriptor descriptor = getDescriptor(dataSource);

    return (queryPlus, responseContext) -> {
      final Iterable<Object[]> authorizedRows = getAuthorizedRows(
          dataSupplier,
          descriptor,
          (ScanQuery) query,
          requestAuthenticationResult
      );
      final Iterable<Object[]> projectedRows = Iterables.transform(
          authorizedRows,
          row -> dataSupplier.projectRow(row, null)
      );
      final ScanQuery resolvedQuery = Druids.ScanQueryBuilder.copy((ScanQuery) query)
                                                       .dataSource(
                                                           InlineDataSource.fromIterable(
                                                               projectedRows,
                                                               descriptor.getRowSignature()
                                                           )
                                                       )
                                                       .build();
      final Segment inlineSegment = new InlineSegmentWrangler()
          .getSegmentsForIntervals(resolvedQuery.getDataSource(), resolvedQuery.getIntervals())
          .iterator()
          .next();

      return runScan(
          scanQueryEngine,
          resolvedQuery,
          inlineSegment,
          queryPlus,
          responseContext
      );
    };
  }

  /** Resolves a local system table directly to lazily projected, user-authorized inline rows. */
  public DataSource resolveDataSource(
      final ScanQuery query,
      final AuthenticationResult requestAuthenticationResult
  )
  {
    final SystemTableDataSource dataSource = (SystemTableDataSource) query.getDataSource();
    final SystemTableDataProvider dataSupplier = getDataSupplier(dataSource);
    final SystemTableDescriptor descriptor = getDescriptor(dataSource);
    final List<String> columns = query.getColumns();
    final int[] projects = new int[columns.size()];
    final RowSignature.Builder signatureBuilder = RowSignature.builder();
    for (int i = 0; i < columns.size(); i++) {
      final String column = columns.get(i);
      final int columnNumber = descriptor.getRowSignature().indexOf(column);
      if (columnNumber < 0) {
        throw new IAE("Column[%s] is not present in system table[%s]", column, dataSource.getTable());
      }
      projects[i] = columnNumber;
      signatureBuilder.add(column, descriptor.getRowSignature().getColumnType(columnNumber).orElse(null));
    }

    final RowSignature projectedSignature = signatureBuilder.build();
    final Iterable<Object[]> authorizedRows = getAuthorizedRows(
        dataSupplier,
        descriptor,
        query,
        requestAuthenticationResult
    );
    final DataSource authorizedDataSource = dataSupplier.getAuthorizedDataSource(
        new SystemTableQueryRequest(
            columns,
            projectedSignature
        ),
        authorizedRows
    ).orElse(null);
    if (authorizedDataSource != null) {
      return authorizedDataSource;
    }

    return InlineDataSource.fromIterable(
        Iterables.transform(authorizedRows, row -> dataSupplier.projectRow(row, projects)),
        projectedSignature
    );
  }

  private Iterable<Object[]> getAuthorizedRows(
      final SystemTableDataProvider dataSupplier,
      final SystemTableDescriptor descriptor,
      final ScanQuery query,
      final AuthenticationResult requestAuthenticationResult
  )
  {
    final Iterable<Object[]> suppliedRows = () -> dataSupplier.getRawRows(
        SystemTablePushdownFilter.extract(query, dataSupplier.getPushdownFilters()),
        requestAuthenticationResult
    ).iterator();
    return descriptor.getRowAuthorizer().filterAuthorizedRows(
        suppliedRows,
        requestAuthenticationResult,
        authorizerMapper
    );
  }

  private SystemTableDataProvider getDataSupplier(final SystemTableDataSource dataSource)
  {
    final SystemTableDataProvider dataSupplier = dataSuppliers.get(dataSource.getTable());
    if (dataSupplier == null) {
      throw new ISE("System table[%s] is not served by this node", dataSource.getTable());
    }
    return dataSupplier;
  }

  private SystemTableDescriptor getDescriptor(final SystemTableDataSource dataSource)
  {
    final SystemTableDescriptor descriptor = tableDescriptors.get(dataSource.getTable());
    if (descriptor == null) {
      throw new ISE("No descriptor is registered for system table[%s]", dataSource.getTable());
    }
    return descriptor;
  }

  @SuppressWarnings("unchecked")
  private static <T> Sequence<T> runScan(
      final ScanQueryEngine scanQueryEngine,
      final ScanQuery query,
      final Segment segment,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    ScanQuery.verifyOrderByForNativeExecution(query);
    initializeTimeout(query, responseContext);
    return (Sequence<T>) (Sequence<?>) scanQueryEngine.process(
        query,
        segment,
        responseContext,
        queryPlus.getQueryMetrics()
    );
  }

  private static void initializeTimeout(final ScanQuery query, final ResponseContext responseContext)
  {
    final Long existingTimeoutAt = responseContext.getTimeoutTime();
    if (existingTimeoutAt != null && existingTimeoutAt != 0L) {
      return;
    }

    final long failTime = query.context().getLong(DirectDruidClient.QUERY_FAIL_TIME, 0L);
    final long timeoutAt;
    if (failTime > 0L) {
      timeoutAt = failTime;
    } else if (query.context().hasTimeout()) {
      timeoutAt = System.currentTimeMillis() + query.context().getTimeout();
    } else {
      timeoutAt = JodaUtils.MAX_INSTANT;
    }
    responseContext.putTimeoutTime(timeoutAt);
  }

}
