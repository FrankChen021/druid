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

package org.apache.druid.server.system;

import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Fetches component rows through native Scan, authorizes them, and executes the original query on the Broker. */
public class NativeSystemQueryClient implements DataSourceQueryHandler
{
  private static final String SYSTEM_TABLE_TIER = "_system";

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final DirectDruidClientFactory directDruidClientFactory;
  private final LocalQuerySegmentWalker localQuerySegmentWalker;
  private final Map<String, NativeSystemTableDescriptor> tableDescriptors;
  private final Map<String, NativeSystemTableRowAuthorizer> rowAuthorizers;

  @Inject
  public NativeSystemQueryClient(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final DirectDruidClientFactory directDruidClientFactory,
      final LocalQuerySegmentWalker localQuerySegmentWalker,
      final Map<String, NativeSystemTableDescriptor> tableDescriptors,
      final Map<String, NativeSystemTableRowAuthorizer> rowAuthorizers
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.directDruidClientFactory = directDruidClientFactory;
    this.localQuerySegmentWalker = localQuerySegmentWalker;
    this.tableDescriptors = tableDescriptors;
    this.rowAuthorizers = rowAuthorizers;
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult authenticationResult
  )
  {
    final SystemTableDataSource dataSource = (SystemTableDataSource) query.getDataSource();
    final NativeSystemTableDescriptor descriptor = tableDescriptors.get(dataSource.getTable());
    if (descriptor == null) {
      throw new ISE("No routing descriptor is registered for system table[%s]", dataSource.getTable());
    }
    final NativeSystemTableRowAuthorizer rowAuthorizer = rowAuthorizers.get(dataSource.getTable());
    if (rowAuthorizer == null) {
      throw new ISE("No row authorizer is registered for system table[%s]", dataSource.getTable());
    }

    final Map<String, Object> componentContext = new LinkedHashMap<>(query.getContext());
    componentContext.put(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_COMPONENT_LOCAL, true);
    componentContext.put(
        DirectDruidClient.QUERY_FAIL_TIME,
        System.currentTimeMillis() + query.context().getTimeout()
    );
    final ScanQuery componentQuery = Druids.newScanQueryBuilder()
                                          .dataSource(dataSource)
                                          .eternityInterval()
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                          .limit(Long.MAX_VALUE)
                                          .filters(componentFilter(query))
                                          .columns(descriptor.getRowSignature())
                                          .context(componentContext)
                                          .build();

    final List<QueryRunner<ScanResultValue>> componentRunners = new ArrayList<>();
    for (final DiscoveryDruidNode discoveryNode : discoverNodes(descriptor)) {
      final QueryRunner<ScanResultValue> directClient = directDruidClientFactory.makeDirectClient(
          toDruidServer(discoveryNode)
      );
      final String componentResourceId = UUID.randomUUID().toString();
      final String componentQueryId = SystemTableDataSource.COMPONENT_QUERY_ID_PREFIX + UUID.randomUUID();
      componentRunners.add(
          (queryPlus, responseContext) -> directClient.run(
              queryPlus.withQuery(
                  componentQuery.withOverriddenContext(
                      Map.of(
                          BaseQuery.QUERY_ID,
                          componentQueryId,
                          QueryContexts.QUERY_RESOURCE_ID,
                          componentResourceId
                      )
                  )
              ),
              responseContext
          )
      );
    }
    if (componentRunners.isEmpty()) {
      throw new ISE("No component is available to serve system table[%s]", dataSource.getTable());
    }

    return (queryPlus, responseContext) -> {
      final List<Object[]> componentRows = new ArrayList<>();
      for (final QueryRunner<ScanResultValue> componentRunner : componentRunners) {
        componentRunner.run(QueryPlus.wrap(componentQuery), responseContext)
                       .accumulate(componentRows, NativeSystemQueryClient::appendRows);
      }

      final List<Object[]> authorizedRows = new ArrayList<>();
      rowAuthorizer.filterAuthorizedRows(componentRows, authenticationResult).forEach(authorizedRows::add);
      final Query<T> resolvedQuery = query.withDataSource(
          InlineDataSource.fromIterable(authorizedRows, descriptor.getRowSignature())
      ).withOverriddenContext(
          Map.of(QueryContexts.QUERY_RESOURCE_ID, UUID.randomUUID().toString())
      );
      final QueryRunner<T> localRunner = localQuerySegmentWalker.getQueryRunnerForIntervals(
          resolvedQuery,
          resolvedQuery.getIntervals()
      );
      return localRunner.run(queryPlus.withQuery(resolvedQuery), responseContext);
    };
  }

  private static List<Object[]> appendRows(final List<Object[]> rows, final ScanResultValue scanResult)
  {
    for (final Object event : (List<?>) scanResult.getEvents()) {
      rows.add(event instanceof Object[] ? (Object[]) event : ((List<?>) event).toArray());
    }
    return rows;
  }

  @Nullable
  private static DimFilter componentFilter(final Query<?> query)
  {
    final List<DimFilter> filters = new ArrayList<>();
    if (query.getFilter() != null) {
      filters.add(query.getFilter());
    }
    if (query instanceof WindowOperatorQuery) {
      for (final OperatorFactory operator : ((WindowOperatorQuery) query).getLeafOperators()) {
        if (operator instanceof ScanOperatorFactory && ((ScanOperatorFactory) operator).getFilter() != null) {
          filters.add(((ScanOperatorFactory) operator).getFilter());
        }
      }
    }
    if (filters.isEmpty()) {
      return null;
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return new AndDimFilter(filters);
    }
  }

  private List<DiscoveryDruidNode> discoverNodes(final NativeSystemTableDescriptor descriptor)
  {
    final Map<String, DiscoveryDruidNode> nodes = new LinkedHashMap<>();
    for (final NodeRole nodeRole : descriptor.getNodeRoles()) {
      for (final DiscoveryDruidNode node : discoveryProvider.getForNodeRole(nodeRole).getAllNodes()) {
        nodes.putIfAbsent(node.getDruidNode().getHostAndPortToUse(), node);
      }
    }
    return new ArrayList<>(nodes.values());
  }

  private static DruidServer toDruidServer(final DiscoveryDruidNode discoveryNode)
  {
    final DruidNode node = discoveryNode.getDruidNode();
    return new DruidServer(
        node.getHostAndPortToUse(),
        node.getHostAndPort(),
        node.getHostAndTlsPort(),
        0,
        null,
        ServerType.HISTORICAL,
        SYSTEM_TABLE_TIER,
        0
    );
  }
}
