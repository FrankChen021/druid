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

import com.google.common.util.concurrent.Futures;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableTargetNodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SystemTableNodeLocatorTest
{
  /**
   * Every discovered node with one of the table's node roles serves the table.
   */
  @Test
  public void testLocatesEveryNodeOfTheTableRoles()
  {
    final DiscoveryDruidNode first = node(8081);
    final DiscoveryDruidNode second = node(8082);
    final SystemTableNodeLocator locator = locator(List.of(first, second), null);
    final SystemTableDescriptor descriptor = descriptor(SystemTableTargetNodes.ALL_NODES);

    final List<SystemTableNode> nodes = locator.locate(descriptor, query());

    Assertions.assertEquals(List.of(first, second), nodes.stream().map(SystemTableNode::getDiscoveryNode).toList());
  }

  /**
   * A leader-only table is read from the current leader alone, not from standby services.
   */
  @Test
  public void testLocatesOnlyTheLeaderForLeaderOnlyTable()
  {
    final DiscoveryDruidNode standby = node(8081);
    final DiscoveryDruidNode leader = node(8082);
    final SystemTableNodeLocator locator = locator(List.of(standby, leader), URI.create("http://localhost:8082"));

    final List<SystemTableNode> nodes = locator.locate(descriptor(SystemTableTargetNodes.LEADER_ONLY), query());

    Assertions.assertEquals(List.of(leader), nodes.stream().map(SystemTableNode::getDiscoveryNode).toList());
  }

  /**
   * A leader that service discovery doesn't know yet makes the table unavailable rather than read from a standby.
   */
  @Test
  public void testRejectsLeaderMissingFromDiscovery()
  {
    final SystemTableNodeLocator locator = locator(List.of(node(8081)), URI.create("http://localhost:8082"));

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> locator.locate(descriptor(SystemTableTargetNodes.LEADER_ONLY), query())
    );
    Assertions.assertEquals(DruidException.Category.SERVICE_UNAVAILABLE, e.getCategory());
  }

  /**
   * Locating the leader respects the query's deadline.
   */
  @Test
  public void testLeaderLookupTimesOutAtQueryDeadline()
  {
    final SystemTableNodeLocator locator = locator(List.of(node(8081)), URI.create("http://localhost:8081"));
    final ScanQuery expiredQuery = query().withOverriddenContext(
        QueryContext.ofMap(QueryContextParameters.QUERY_FAIL_TIME, System.currentTimeMillis() - 1)
    );

    Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> locator.locate(descriptor(SystemTableTargetNodes.LEADER_ONLY), expiredQuery)
    );
  }

  /**
   * A failed leader lookup makes the table unavailable.
   */
  @Test
  public void testLeaderLookupFailureIsServiceUnavailable()
  {
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    Mockito.when(overlordClient.findCurrentLeader())
           .thenReturn(Futures.immediateFailedFuture(new IOException("no leader")));
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(discoveryProvider(List.of(node(8081))), overlordClient);

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> locator.locate(descriptor(SystemTableTargetNodes.LEADER_ONLY), query())
    );
    Assertions.assertEquals(DruidException.Category.SERVICE_UNAVAILABLE, e.getCategory());
  }

  /**
   * While no leader is elected, the Overlord reports an empty leader, which makes the table unavailable.
   */
  @Test
  public void testNoElectedLeaderIsServiceUnavailable()
  {
    for (final URI leader : Arrays.asList(URI.create(""), null)) {
      final SystemTableNodeLocator locator = locator(List.of(node(8081)), leader);

      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> locator.locate(descriptor(SystemTableTargetNodes.LEADER_ONLY), query())
      );
      Assertions.assertEquals(DruidException.Category.SERVICE_UNAVAILABLE, e.getCategory());
    }
  }

  private static SystemTableNodeLocator locator(
      final List<DiscoveryDruidNode> nodes,
      @Nullable final URI overlordLeader
  )
  {
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    Mockito.when(overlordClient.findCurrentLeader()).thenReturn(Futures.immediateFuture(overlordLeader));
    return new SystemTableNodeLocator(discoveryProvider(nodes), overlordClient);
  }

  private static SystemTableDescriptor descriptor(final SystemTableTargetNodes targetNodes)
  {
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getNodeRoles()).thenReturn(Set.of(NodeRole.OVERLORD));
    Mockito.when(descriptor.getTargetNodes()).thenReturn(targetNodes);
    return descriptor;
  }

  private static ScanQuery query()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource("tasks"))
                 .eternityInterval()
                 .context(QueryContext.ofMap(QueryContextParameters.QUERY_FAIL_TIME, Long.MAX_VALUE))
                 .build();
  }

  private static DruidNodeDiscoveryProvider discoveryProvider(final List<DiscoveryDruidNode> nodes)
  {
    return discoveryProvider(NodeRole.OVERLORD, nodes);
  }

  private static DruidNodeDiscoveryProvider discoveryProvider(
      final NodeRole nodeRole,
      final List<DiscoveryDruidNode> nodes
  )
  {
    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenReturn(nodes);
    final DruidNodeDiscoveryProvider provider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    Mockito.when(provider.getForNodeRole(nodeRole)).thenReturn(discovery);
    return provider;
  }

  private static DiscoveryDruidNode node(final int port)
  {
    return node(NodeRole.OVERLORD, port);
  }

  private static DiscoveryDruidNode node(final NodeRole nodeRole, final int port)
  {
    return new DiscoveryDruidNode(
        new DruidNode(nodeRole.getJsonName(), "localhost", false, port, null, true, false),
        nodeRole,
        Collections.emptyMap()
    );
  }

}
