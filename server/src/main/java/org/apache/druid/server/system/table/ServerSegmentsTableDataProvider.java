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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Native row supplier for {@code sys.server_segments}. */
public class ServerSegmentsTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", null),
      new SystemTablePushdownFilter("segment_id", null),
      new SystemTablePushdownFilter("datasource", null)
  );

  private final TimelineServerView serverView;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public ServerSegmentsTableDataProvider(
      final TimelineServerView serverView,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.serverView = serverView;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Optional<DataSource> getAuthorizedDataSource(
      final SystemTableQueryRequest request,
      final Iterable<Object[]> authorizedRows
  )
  {
    final int[] projects = request.columns()
                                  .stream()
                                  .mapToInt(ServerSegmentsTableDescriptor.ROW_SIGNATURE::indexOf)
                                  .toArray();
    return Optional.of(
        new BatchedInlineDataSource(
            Iterables.transform(authorizedRows, row -> projectRow(row, projects)),
            request.rowSignature()
        )
    );
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult authenticationResult
  )
  {
    final Set<String> serverFilter = getStringFilter(filters, "server");
    final Set<String> segmentFilter = getStringFilter(filters, "segment_id");
    final Set<String> dataSourceFilter = getStringFilter(filters, "datasource");
    final Iterable<ImmutableDruidServer> servers = serverFilter == null
                                                   ? serverView.getDruidServers()
                                                   : Iterables.filter(
                                                       serverView.getDruidServers(),
                                                       server -> serverFilter.contains(server.getHost())
                                                   );
    final Iterable<Iterable<Object[]>> rowsByServer = Iterables.transform(
        servers,
        server -> getAuthorizedRows(server, segmentFilter, dataSourceFilter, authenticationResult)
    );
    return Iterables.concat(rowsByServer);
  }

  private Iterable<Object[]> getAuthorizedRows(
      final ImmutableDruidServer server,
      @Nullable final Set<String> segmentFilter,
      @Nullable final Set<String> dataSourceFilter,
      final AuthenticationResult authenticationResult
  )
  {
    final Iterable<ImmutableDruidDataSource> dataSources = dataSourceFilter == null
                                                           ? server.getDataSources()
                                                           : Iterables.filter(
                                                               server.getDataSources(),
                                                               dataSource -> dataSourceFilter.contains(
                                                                   dataSource.getName()
                                                               )
                                                           );
    final Iterable<DataSegment> segments = Iterables.concat(
        Iterables.transform(dataSources, ImmutableDruidDataSource::getSegments)
    );
    final Iterable<DataSegment> filteredSegments = segmentFilter == null
                                                   ? segments
                                                   : Iterables.filter(
                                                       segments,
                                                       segment -> segmentFilter.contains(segment.getId().toString())
                                                   );
    final Iterable<DataSegment> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        filteredSegments,
        segment -> Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSource())
        ),
        authorizerMapper
    );
    return Iterables.transform(
        authorizedSegments,
        segment -> new Object[]{server.getHost(), segment.getId().toString(), segment.getDataSource()}
    );
  }

  @Nullable
  private static Set<String> getStringFilter(final List<DimFilter> filters, final String column)
  {
    Set<String> result = null;
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter)
          && column.equals(SystemTablePushdownFilter.getStringValuesColumn(filter))) {
        final Set<String> values = SystemTablePushdownFilter.getStringValues(filter);
        result = result == null ? values : Sets.intersection(result, values).immutableCopy();
      }
    }
    return result;
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    if (filter instanceof SelectorDimFilter
        || filter instanceof EqualityFilter
        || filter instanceof InDimFilter
        || filter instanceof TypedInFilter) {
      return true;
    }
    if (filter instanceof OrDimFilter or) {
      return !or.getFields().isEmpty()
             && or.getFields().stream().allMatch(ServerSegmentsTableDataProvider::isStringValuesFilter);
    }
    return false;
  }
}
