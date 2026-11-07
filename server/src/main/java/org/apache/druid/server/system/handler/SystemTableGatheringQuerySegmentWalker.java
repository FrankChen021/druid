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

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.system.SystemTableQuerySegmentWalker;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

/**
 * The Broker's walker when system tables are enabled. Before a query that reads system tables
 * {@link SystemTableDataSource#isGatheredFromAllServices() gathered from all services} runs, it replaces them with the
 * rows that the caller may read, gathered from every service by {@link SystemTableQueryClient}, and then passes the
 * query to the Broker's own walker. Every other query, including a native query on a system table, which reads only
 * the Broker's rows, goes to the Broker's walker unchanged.
 *
 * <p>The rows are gathered after {@link org.apache.druid.server.QueryLifecycle} has authorized and logged the query,
 * so the request log, metrics and query blocklist see the system table rather than its rows.</p>
 */
public class SystemTableGatheringQuerySegmentWalker implements QuerySegmentWalker
{
  private final QuerySegmentWalker delegate;
  private final SystemTableQueryClient systemTableQueryClient;

  @Inject
  public SystemTableGatheringQuerySegmentWalker(
      @SystemTableQuerySegmentWalker.Delegate final QuerySegmentWalker delegate,
      final SystemTableQueryClient systemTableQueryClient
  )
  {
    this.delegate = delegate;
    this.systemTableQueryClient = systemTableQueryClient;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    if (!readsGatheredSystemTable(query.getDataSource())) {
      return delegate.getQueryRunnerForIntervals(query, intervals);
    }
    return (queryPlus, responseContext) -> {
      final SystemTableQueryClient.ResolvedQuery<T> resolvedQuery =
          systemTableQueryClient.resolve(queryPlus.getQuery());
      try {
        final Query<T> resolved = resolvedQuery.query();
        return Sequences.withBaggage(
            delegate.getQueryRunnerForIntervals(resolved, resolved.getIntervals())
                    .run(queryPlus.withQuery(resolved), responseContext),
            resolvedQuery.closer()
        );
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, resolvedQuery.closer());
      }
    };
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (readsGatheredSystemTable(query.getDataSource())) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build("System tables can't be queried for specific segments");
    }
    return delegate.getQueryRunnerForSegments(query, specs);
  }

  private static boolean readsGatheredSystemTable(final DataSource dataSource)
  {
    if (dataSource instanceof SystemTableDataSource systemTable) {
      return systemTable.isGatheredFromAllServices();
    }
    for (final DataSource child : dataSource.getChildren()) {
      if (readsGatheredSystemTable(child)) {
        return true;
      }
    }
    return false;
  }
}
