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

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SystemTableNodeLocatorTest
{
  /** ALL_NODES discovers every configured role without consulting either leader client. */
  @Test
  public void testLocateAllNodes()
  {
    final DiscoveryDruidNode broker = node("broker", "broker.example.com", 8082, NodeRole.BROKER);
    final DiscoveryDruidNode historical = node("historical", "historical.example.com", 8083, NodeRole.HISTORICAL);
    final DruidNodeDiscoveryProvider discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    final DruidNodeDiscovery brokerDiscovery = discovery(List.of(broker));
    final DruidNodeDiscovery historicalDiscovery = discovery(List.of(historical));
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(brokerDiscovery);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.HISTORICAL)).thenReturn(historicalDiscovery);
    final CoordinatorClient coordinatorClient = Mockito.mock(CoordinatorClient.class);
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    final SystemTableDescriptor descriptor = descriptor(
        SystemTableRoutingMode.ALL_NODES,
        Set.of(NodeRole.BROKER, NodeRole.HISTORICAL)
    );

    final List<SystemTableNode> nodes = new SystemTableNodeLocator(
        discoveryProvider,
        coordinatorClient,
        overlordClient
    ).locate(descriptor, Long.MAX_VALUE);

    Assertions.assertEquals(
        Set.of("broker.example.com:8082", "historical.example.com:8083"),
        nodes.stream()
             .map(node -> node.getDiscoveryNode().getDruidNode().getHostAndPortToUse())
             .collect(Collectors.toSet())
    );
    Mockito.verifyNoInteractions(coordinatorClient, overlordClient);
  }

  /** LEADER_ONLY resolves leadership first and inspects discovery for that role only. */
  @Test
  public void testLocateLeaderOnly()
  {
    final DiscoveryDruidNode follower = node("overlord", "follower.example.com", 8090, NodeRole.OVERLORD);
    final DiscoveryDruidNode leader = node("overlord", "leader.example.com", 8090, NodeRole.OVERLORD);
    final DruidNodeDiscoveryProvider discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    final DruidNodeDiscovery overlordDiscovery = discovery(List.of(follower, leader));
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.OVERLORD))
           .thenReturn(overlordDiscovery);
    final CoordinatorClient coordinatorClient = Mockito.mock(CoordinatorClient.class);
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    Mockito.when(overlordClient.findCurrentLeader())
           .thenReturn(Futures.immediateFuture(URI.create("http://leader.example.com:8090")));
    final SystemTableDescriptor descriptor = descriptor(
        SystemTableRoutingMode.LEADER_ONLY,
        Set.of(NodeRole.OVERLORD)
    );

    final List<SystemTableNode> nodes = new SystemTableNodeLocator(
        discoveryProvider,
        coordinatorClient,
        overlordClient
    ).locate(descriptor, Long.MAX_VALUE);

    Assertions.assertEquals(List.of(leader), nodes.stream().map(SystemTableNode::getDiscoveryNode).toList());
    Mockito.verify(discoveryProvider).getForNodeRole(NodeRole.OVERLORD);
    Mockito.verifyNoMoreInteractions(discoveryProvider);
    Mockito.verifyNoInteractions(coordinatorClient);
  }

  private static SystemTableDescriptor descriptor(
      final SystemTableRoutingMode routingMode,
      final Set<NodeRole> nodeRoles
  )
  {
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getRoutingMode()).thenReturn(routingMode);
    Mockito.when(descriptor.getNodeRoles()).thenReturn(nodeRoles);
    return descriptor;
  }

  private static DruidNodeDiscovery discovery(final List<DiscoveryDruidNode> nodes)
  {
    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenReturn(nodes);
    return discovery;
  }

  private static DiscoveryDruidNode node(
      final String service,
      final String host,
      final int port,
      final NodeRole role
  )
  {
    return new DiscoveryDruidNode(
        new DruidNode(service, host, false, port, -1, true, false),
        role,
        Map.of()
    );
  }
}
