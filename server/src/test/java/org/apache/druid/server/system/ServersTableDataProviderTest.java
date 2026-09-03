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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.system.table.ServersTableDataProvider;
import org.apache.druid.server.system.table.ServersTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ServersTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  @Test
  public void testReturnsDiscoveredServersWithStringLabels() throws Exception
  {
    final DruidNodeDiscoveryProvider discoveryProvider = EasyMock.mock(DruidNodeDiscoveryProvider.class);
    final FilteredServerInventoryView serverInventoryView = EasyMock.mock(FilteredServerInventoryView.class);
    final CoordinatorClient coordinatorClient = EasyMock.mock(CoordinatorClient.class);
    final OverlordClient overlordClient = EasyMock.mock(OverlordClient.class);

    final DiscoveryDruidNode coordinator = discoveryNode(
        new DruidNode("coordinator", "localhost", false, 8081, null, true, false),
        NodeRole.COORDINATOR,
        Collections.emptyMap()
    );
    final Map<String, String> labels = ImmutableMap.of("environment", "test");
    final DiscoveryDruidNode broker = discoveryNode(
        new DruidNode("broker", "localhost", false, 8082, null, null, true, false, labels),
        NodeRole.BROKER,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY,
            new DataNodeService("tier", 1_000L, 900L, ServerType.BROKER, 0)
        )
    );

    expectDiscoveryNodes(discoveryProvider, NodeRole.COORDINATOR, coordinator);
    expectDiscoveryNodes(discoveryProvider, NodeRole.BROKER, broker);
    for (final NodeRole nodeRole : NodeRole.values()) {
      if (!NodeRole.COORDINATOR.equals(nodeRole) && !NodeRole.BROKER.equals(nodeRole)) {
        expectDiscoveryNodes(discoveryProvider, nodeRole);
      }
    }

    final DruidServer server = EasyMock.mock(DruidServer.class);
    EasyMock.expect(serverInventoryView.getInventoryValue("localhost:8082")).andReturn(server).once();
    EasyMock.expect(server.getCurrSize()).andReturn(100L).once();
    EasyMock.expect(server.getTier()).andReturn("tier").once();
    EasyMock.expect(server.getMaxSize()).andReturn(1_000L).once();
    EasyMock.expect(server.getStorageSize()).andReturn(900L).once();
    EasyMock.expect(coordinatorClient.findCurrentLeader())
            .andReturn(Futures.immediateFuture(new URI("localhost:8081")))
            .once();
    EasyMock.expect(overlordClient.findCurrentLeader())
            .andReturn(Futures.immediateFuture(new URI("localhost:8090")))
            .once();

    EasyMock.replay(discoveryProvider, serverInventoryView, coordinatorClient, overlordClient, server);

    final ServersTableDataProvider provider = new ServersTableDataProvider(
        discoveryProvider,
        serverInventoryView,
        allowAllAuthorizerMapper(),
        overlordClient,
        coordinatorClient,
        TestHelper.JSON_MAPPER
    );
    final List<Object[]> rows = toRows(provider.getRows(Collections.emptyList(), AUTHENTICATION_RESULT));

    final Object[] coordinatorRow = rows.stream()
                                        .filter(row -> "localhost:8081".equals(row[0]))
                                        .findFirst()
                                        .orElseThrow();
    Assertions.assertEquals(1L, coordinatorRow[9]);
    Assertions.assertNull(coordinatorRow[13]);

    final Object[] brokerRow = rows.stream()
                                   .filter(row -> "localhost:8082".equals(row[0]))
                                   .findFirst()
                                   .orElseThrow();
    Assertions.assertEquals(TestHelper.JSON_MAPPER.writeValueAsString(labels), brokerRow[13]);
    Assertions.assertEquals(100L, brokerRow[6]);
    Assertions.assertEquals(ColumnType.STRING, ServersTableDescriptor.ROW_SIGNATURE.getColumnType(13).orElseThrow());

    EasyMock.verify(discoveryProvider, serverInventoryView, coordinatorClient, overlordClient, server);
  }

  @Test
  public void testRejectsUnauthorizedRequest()
  {
    final Authorizer denyAll = (authenticationResult, resource, action) -> Access.DENIED;
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return denyAll;
      }
    };
    final ServersTableDataProvider provider = new ServersTableDataProvider(
        EasyMock.mock(DruidNodeDiscoveryProvider.class),
        EasyMock.mock(FilteredServerInventoryView.class),
        authorizerMapper,
        EasyMock.mock(OverlordClient.class),
        EasyMock.mock(CoordinatorClient.class),
        TestHelper.JSON_MAPPER
    );

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> provider.getRows(Collections.emptyList(), AUTHENTICATION_RESULT)
    );
  }

  @Test
  public void testDescriptorRunsLocallyOnBroker()
  {
    final ServersTableDescriptor descriptor = new ServersTableDescriptor();

    Assertions.assertEquals(Set.of(NodeRole.BROKER), descriptor.getNodeRoles());
    Assertions.assertEquals(SystemTableRoutingMode.LOCAL, descriptor.getRoutingMode());
    Assertions.assertEquals(ColumnType.STRING, descriptor.getRowSignature().getColumnType(13).orElseThrow());
  }

  private static DiscoveryDruidNode discoveryNode(
      final DruidNode node,
      final NodeRole nodeRole,
      final Map<String, DruidService> services
  )
  {
    return new DiscoveryDruidNode(node, nodeRole, services, null);
  }

  private static void expectDiscoveryNodes(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final NodeRole nodeRole,
      final DiscoveryDruidNode... nodes
  )
  {
    final DruidNodeDiscovery discovery = EasyMock.mock(DruidNodeDiscovery.class);
    EasyMock.expect(discoveryProvider.getForNodeRole(nodeRole)).andReturn(discovery).once();
    EasyMock.expect(discovery.getAllNodes()).andReturn(List.of(nodes)).once();
    EasyMock.replay(discovery);
  }

  private static AuthorizerMapper allowAllAuthorizerMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) -> Access.OK;
      }
    };
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
