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

package org.apache.druid.server.system.query;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Finds the Druid services that serve a system table: every discovered service with one of the table's node roles, or
 * only the current leader for a {@link SystemTableRoutingMode#LEADER_ONLY} table.
 */
public class SystemTableNodeLocator
{
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final Map<NodeRole, Supplier<ListenableFuture<URI>>> leaderLocators;

  @Inject
  public SystemTableNodeLocator(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final OverlordClient overlordClient
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.leaderLocators = Map.of(NodeRole.OVERLORD, overlordClient::findCurrentLeader);
  }

  /**
   * Returns the services to read the table from. For a {@link SystemTableRoutingMode#LEADER_ONLY} table, this is the
   * current leader, located within the deadline of the given query.
   */
  List<SystemTableNode> locate(final SystemTableDescriptor descriptor, final Query<?> query)
  {
    if (descriptor.getRoutingMode() != SystemTableRoutingMode.LEADER_ONLY) {
      return discoverAllNodes(descriptor);
    }

    final NodeRole leaderRole = Iterables.getOnlyElement(descriptor.getNodeRoles());
    final URI leaderUri = findLeader(leaderRole, query);
    // Resolve leadership before taking the discovery snapshot. A leader election may complete while discovery still
    // contains the previous membership, so taking the snapshot first can reject a valid newly elected leader.
    return discoverAllNodes(descriptor).stream()
                                       .filter(node -> sameServer(
                                           leaderUri,
                                           node.getDiscoveryNode().getDruidNode().getUriToUse()
                                       ))
                                       .findFirst()
                                       .map(List::of)
                                       .orElseThrow(
                                           () -> DruidException.forPersona(DruidException.Persona.OPERATOR)
                                                               .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                                                               .build(
                                                                   "Current leader[%s] for role[%s] is not present in"
                                                                   + " service discovery",
                                                                   leaderUri,
                                                                   leaderRole
                                                               )
                                       );
  }

  private List<SystemTableNode> discoverAllNodes(final SystemTableDescriptor descriptor)
  {
    final Map<String, SystemTableNode> nodes = new LinkedHashMap<>();
    for (final NodeRole nodeRole : descriptor.getNodeRoles()) {
      for (final DiscoveryDruidNode node : discoveryProvider.getForNodeRole(nodeRole).getAllNodes()) {
        nodes.computeIfAbsent(
            node.getDruidNode().getHostAndPortToUse(),
            ignored -> new SystemTableNode(node)
        ).addNodeRole(node.getNodeRole());
      }
    }
    return new ArrayList<>(nodes.values());
  }

  private URI findLeader(final NodeRole nodeRole, final Query<?> query)
  {
    final Supplier<ListenableFuture<URI>> leaderLocator = leaderLocators.get(nodeRole);
    if (leaderLocator == null) {
      throw DruidException.defensive("Leader-only system-table routing is not supported for role[%s]", nodeRole);
    }

    final ListenableFuture<URI> leaderFuture = leaderLocator.get();
    final long timeLeft = query.context().get(QueryContextParameters.QUERY_FAIL_TIME) - System.currentTimeMillis();
    if (timeLeft <= 0) {
      leaderFuture.cancel(true);
      throw new QueryTimeoutException("Timed out while locating the current leader for role[" + nodeRole + "]");
    }
    try {
      return requireLeader(leaderFuture.get(timeLeft, TimeUnit.MILLISECONDS), nodeRole);
    }
    catch (InterruptedException e) {
      leaderFuture.cancel(true);
      Thread.currentThread().interrupt();
      throw QueryInterruptedException.wrapIfNeeded(e);
    }
    catch (TimeoutException e) {
      leaderFuture.cancel(true);
      throw new QueryTimeoutException("Timed out while locating the current leader for role[" + nodeRole + "]");
    }
    catch (ExecutionException e) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build(
                              e.getCause() == null ? e : e.getCause(),
                              "Could not locate the current leader for role[%s]",
                              nodeRole
                          );
    }
  }

  /**
   * Returns the leader URI, or throws {@code SERVICE_UNAVAILABLE} when the Overlord reports no leader, as an empty URI,
   * while an election is running.
   */
  private static URI requireLeader(@Nullable final URI leaderUri, final NodeRole nodeRole)
  {
    if (leaderUri == null || leaderUri.getScheme() == null || leaderUri.getHost() == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("No current leader for role[%s]", nodeRole);
    }
    return leaderUri;
  }

  static boolean sameServer(final URI first, final URI second)
  {
    return first.getScheme().equalsIgnoreCase(second.getScheme())
           && first.getHost().equalsIgnoreCase(second.getHost())
           && effectivePort(first) == effectivePort(second);
  }

  private static int effectivePort(final URI uri)
  {
    if (uri.getPort() >= 0) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }
}
