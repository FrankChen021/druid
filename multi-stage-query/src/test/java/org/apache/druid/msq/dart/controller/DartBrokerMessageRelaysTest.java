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


package org.apache.druid.msq.dart.controller;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.messages.client.MessageRelay;
import org.apache.druid.messages.client.MessageRelayFactory;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DartBrokerMessageRelaysTest
{
  private static final DruidNode SELF = new DruidNode("broker", "broker-1", false, 8082, -1, true, false);
  private static final DruidNode OTHER = new DruidNode("broker", "broker-2", false, 8082, -1, true, false);

  /**
   * Only this Broker's own embedded worker takes part in its queries, so a Broker relays messages from itself and
   * not from every other Broker: relaying from all of them would open N long-poll connections per Broker, all of which
   * retry forever against Brokers that do not run a Dart worker.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRelaysOnlyFromSelf()
  {
    final List<DruidNodeDiscovery.Listener> listeners = new ArrayList<>();
    final DruidNodeDiscovery discovery = new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return Collections.emptyList();
      }

      @Override
      public void registerListener(final Listener listener)
      {
        listeners.add(listener);
      }

      @Override
      public void removeListener(final Listener listener)
      {
        listeners.remove(listener);
      }
    };
    final DruidNodeDiscoveryProvider discoveryProvider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    final MessageRelayFactory<ControllerMessage> relayFactory = Mockito.mock(MessageRelayFactory.class);
    Mockito.when(relayFactory.newRelay(Mockito.any())).thenReturn(Mockito.mock(MessageRelay.class));

    final DartBrokerMessageRelays relays = new DartBrokerMessageRelays(discoveryProvider, SELF, relayFactory);
    relays.start();
    listeners.forEach(
        listener -> listener.nodesAdded(
            List.of(
                new DiscoveryDruidNode(SELF, NodeRole.BROKER, Collections.emptyMap()),
                new DiscoveryDruidNode(OTHER, NodeRole.BROKER, Collections.emptyMap())
            )
        )
    );

    Mockito.verify(relayFactory).newRelay(SELF);
    Mockito.verifyNoMoreInteractions(relayFactory);
    relays.stop();
  }
}
