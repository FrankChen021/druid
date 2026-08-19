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

package org.apache.druid.indexing.overlord.http;

import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.Segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Resolves one component-local system table and returns its rows through the standard native Scan stack. */
public class NativeSystemTableQueryHandler implements DataSourceQueryHandler
{
  private final Map<String, NativeSystemTableDataSupplier> dataSuppliers;
  private final ScanQueryEngine scanQueryEngine;
  private final ServiceEmitter emitter;
  private final AuthenticationResult internalAuthenticationResult;

  @Inject
  public NativeSystemTableQueryHandler(
      final Map<String, NativeSystemTableDataSupplier> dataSuppliers,
      final ScanQueryEngine scanQueryEngine,
      final ServiceEmitter emitter,
      final Escalator escalator
  )
  {
    this.dataSuppliers = dataSuppliers;
    this.scanQueryEngine = scanQueryEngine;
    this.emitter = emitter;
    this.internalAuthenticationResult = escalator.createEscalatedAuthenticationResult();
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult requestAuthenticationResult
  )
  {
    if (!query.context().getBoolean(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_COMPONENT_LOCAL, false)) {
      throw new IAE("System tables can only be resolved by an internal component query");
    }
    if (!(query instanceof ScanQuery)) {
      throw new IAE("Component-local system table queries must be scan queries");
    }
    if (!internalAuthenticationResult.getIdentity().equals(requestAuthenticationResult.getIdentity())) {
      throw new IAE("Component-local system table queries require the internal system identity");
    }

    final SystemTableDataSource dataSource = (SystemTableDataSource) query.getDataSource();
    final NativeSystemTableDataSupplier dataSupplier = dataSuppliers.get(dataSource.getTable());
    if (dataSupplier == null) {
      throw new ISE("System table[%s] is not served by this component", dataSource.getTable());
    }

    final List<Object[]> rows = new ArrayList<>();
    dataSupplier.getRows(
        NativeSystemTableFilterExtractor.extract(query, dataSupplier.getFilterRules()),
        requestAuthenticationResult
    ).forEach(rows::add);

    final ScanQuery resolvedQuery = Druids.ScanQueryBuilder.copy((ScanQuery) query)
                                                     .dataSource(
                                                         InlineDataSource.fromIterable(
                                                             rows,
                                                             dataSupplier.getRowSignature()
                                                         )
                                                     )
                                                     .filters(null)
                                                     .build();
    final Segment inlineSegment = new InlineSegmentWrangler()
        .getSegmentsForIntervals(resolvedQuery.getDataSource(), resolvedQuery.getIntervals())
        .iterator()
        .next();

    emitter.emit(
        ServiceMetricEvent.builder().setMetric(
            "query/" + metricPrefix(dataSource.getTable()) + "/rowsRead",
            rows.size()
        )
    );
    return (queryPlus, responseContext) -> runScan(
        scanQueryEngine,
        resolvedQuery,
        inlineSegment,
        queryPlus,
        responseContext
    );
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

  private static String metricPrefix(final String table)
  {
    final StringBuilder metricPrefix = new StringBuilder("system");
    boolean capitalize = true;
    for (int i = 0; i < table.length(); i++) {
      final char character = table.charAt(i);
      if (character == '_') {
        capitalize = true;
      } else {
        metricPrefix.append(capitalize ? Character.toUpperCase(character) : character);
        capitalize = false;
      }
    }
    return metricPrefix.toString();
  }
}
