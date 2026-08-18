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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Resolves one component-local system table and returns its rows through the standard native Scan stack. */
public class NativeSystemTableQueryHandler implements DataSourceQueryHandler
{
  private final Map<String, NativeSystemTableDataSupplier> dataSuppliers;
  private final LocalQuerySegmentWalker localQuerySegmentWalker;
  private final ServiceEmitter emitter;
  private final AuthenticationResult internalAuthenticationResult;

  @Inject
  public NativeSystemTableQueryHandler(
      final Map<String, NativeSystemTableDataSupplier> dataSuppliers,
      final LocalQuerySegmentWalker localQuerySegmentWalker,
      final ServiceEmitter emitter,
      final Escalator escalator
  )
  {
    this.dataSuppliers = dataSuppliers;
    this.localQuerySegmentWalker = localQuerySegmentWalker;
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
    @SuppressWarnings("unchecked")
    final Query<T> typedResolvedQuery = (Query<T>) (Query<?>) resolvedQuery;
    final QueryRunner<T> localRunner = localQuerySegmentWalker.getQueryRunnerForIntervals(
        typedResolvedQuery,
        resolvedQuery.getIntervals()
    );

    emitter.emit(
        ServiceMetricEvent.builder().setMetric(
            "query/" + metricPrefix(dataSource.getTable()) + "/rowsRead",
            rows.size()
        )
    );
    return (queryPlus, responseContext) -> localRunner.run(
        queryPlus.withQuery(typedResolvedQuery),
        responseContext
    );
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
