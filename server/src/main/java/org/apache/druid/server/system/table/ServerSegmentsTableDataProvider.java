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
import com.google.inject.Inject;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.query.filter.Filter;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Native row supplier for {@code sys.server_segments}.
 */
public class ServerSegmentsTableDataProvider implements SystemTableDataProvider
{
  private static final Set<String> PUSHDOWN_COLUMNS = Set.of("server", "segment_id", "datasource");

  private final TimelineServerView serverView;

  @Inject
  public ServerSegmentsTableDataProvider(final TimelineServerView serverView)
  {
    this.serverView = serverView;
  }

  @Override
  public Set<String> getPushdownColumns()
  {
    return PUSHDOWN_COLUMNS;
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters)
  {
    final Map<String, Set<String>> stringValues = SystemTableStringFilters.getStringValues(filters);
    if (stringValues.values().stream().anyMatch(Set::isEmpty)) {
      // The filters allow no value of some column, such as datasource = 'a' AND datasource = 'b'.
      return List.of();
    }
    final Set<String> serverFilter = stringValues.get("server");
    final Set<String> segmentFilter = stringValues.get("segment_id");
    final Set<String> dataSourceFilter = stringValues.get("datasource");
    final Iterable<ImmutableDruidServer> servers = Iterables.filter(
        serverView.getDruidServers(),
        server -> SystemTableStringFilters.matches(serverFilter, server.getHost())
    );
    return Iterables.concat(
        Iterables.transform(servers, server -> getServerRows(server, segmentFilter, dataSourceFilter))
    );
  }

  private static Iterable<Object[]> getServerRows(
      final ImmutableDruidServer server,
      @Nullable final Set<String> segmentFilter,
      @Nullable final Set<String> dataSourceFilter
  )
  {
    final Iterable<ImmutableDruidDataSource> dataSources = dataSourceFilter == null
                                                           ? server.getDataSources()
                                                           : Iterables.filter(
                                                               Iterables.transform(
                                                                   dataSourceFilter,
                                                                   server::getDataSource
                                                               ),
                                                               Objects::nonNull
                                                           );
    final Iterable<Object[]> rows = Iterables.transform(
        Iterables.concat(Iterables.transform(dataSources, ImmutableDruidDataSource::getSegments)),
        segment -> new Object[]{server.getHost(), segment.getId().toString(), segment.getDataSource()}
    );
    return Iterables.filter(rows, row -> SystemTableStringFilters.matches(segmentFilter, (String) row[1]));
  }
}
