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

package org.apache.druid.server;

import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.FluentQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthenticationResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Distributes a native system-table query to its owning components through their standard {@code /druid/v2} API. */
public class NativeSystemQueryClient implements DataSourceQueryHandler
{
  private static final String SYSTEM_TABLE_TIER = "_system";

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final DirectDruidClientFactory directDruidClientFactory;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryScheduler queryScheduler;
  private final ServiceEmitter emitter;
  private final Map<String, NativeSystemTableDescriptor> tableDescriptors;

  @Inject
  public NativeSystemQueryClient(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final DirectDruidClientFactory directDruidClientFactory,
      final QueryRunnerFactoryConglomerate conglomerate,
      final QueryScheduler queryScheduler,
      final ServiceEmitter emitter,
      final Map<String, NativeSystemTableDescriptor> tableDescriptors
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.directDruidClientFactory = directDruidClientFactory;
    this.conglomerate = conglomerate;
    this.queryScheduler = queryScheduler;
    this.emitter = emitter;
    this.tableDescriptors = tableDescriptors;
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

    final Map<String, Object> componentContext = new LinkedHashMap<>();
    componentContext.put(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_COMPONENT_LOCAL, true);
    componentContext.put(SystemTableDataSource.CTX_AUTHENTICATION_IDENTITY, authenticationResult.getIdentity());
    if (authenticationResult.getAuthorizerName() != null) {
      componentContext.put(
          SystemTableDataSource.CTX_AUTHENTICATION_AUTHORIZER,
          authenticationResult.getAuthorizerName()
      );
    }
    if (authenticationResult.getAuthenticatedBy() != null) {
      componentContext.put(SystemTableDataSource.CTX_AUTHENTICATED_BY, authenticationResult.getAuthenticatedBy());
    }
    if (authenticationResult.getContext() != null) {
      componentContext.put(SystemTableDataSource.CTX_AUTHENTICATION_CONTEXT, authenticationResult.getContext());
    }
    componentContext.put(
        DirectDruidClient.QUERY_FAIL_TIME,
        System.currentTimeMillis() + query.context().getTimeout()
    );
    final Query<T> distributedQuery = query.withOverriddenContext(componentContext);

    final List<QueryRunner<T>> componentRunners = new ArrayList<>();
    for (final DiscoveryDruidNode discoveryNode : discoverNodes(descriptor)) {
      final QueryRunner<T> directClient = directDruidClientFactory.makeDirectClient(toDruidServer(discoveryNode));
      final String componentResourceId = UUID.randomUUID().toString();
      final String componentQueryId = SystemTableDataSource.COMPONENT_QUERY_ID_PREFIX + UUID.randomUUID();
      componentRunners.add(
          (queryPlus, responseContext) -> directClient.run(
              queryPlus.withQuery(
                  queryPlus.getQuery().withOverriddenContext(
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

    final Query<T> mergeQuery = distributedQuery.withOverriddenContext(
        Map.of(QueryContexts.QUERY_RESOURCE_ID, UUID.randomUUID().toString())
    );
    final QueryRunnerFactory<T, Query<T>> queryRunnerFactory = conglomerate.findFactory(mergeQuery);
    final QueryRunner<T> mergedRunner = queryRunnerFactory.mergeRunners(
        DirectQueryProcessingPool.INSTANCE,
        componentRunners
    );
    final AtomicLong cpuAccumulator = new AtomicLong();
    final QueryRunner<T> decoratedRunner = FluentQueryRunner
        .create(queryScheduler.wrapQueryRunner(mergedRunner), queryRunnerFactory.getToolchest())
        .applyPreMergeDecoration()
        .mergeResults(true)
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter, cpuAccumulator);

    return (queryPlus, responseContext) -> decoratedRunner.run(
        queryPlus.withQuery(mergeQuery),
        responseContext
    );
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
