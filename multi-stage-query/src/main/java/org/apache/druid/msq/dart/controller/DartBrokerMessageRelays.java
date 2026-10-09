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
import org.apache.druid.messages.client.MessageRelayFactory;
import org.apache.druid.messages.client.MessageRelays;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.server.DruidNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Receives controller messages from the Dart worker embedded in this Broker. Only this Broker's own embedded worker
 * takes part in its queries, so unlike {@link DartMessageRelays} this relays from the local node alone rather than from
 * every Broker, which would keep long-poll connections open to workers that never talk to this controller.
 */
public class DartBrokerMessageRelays extends MessageRelays<ControllerMessage>
{
  public DartBrokerMessageRelays(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final DruidNode selfNode,
      final MessageRelayFactory<ControllerMessage> messageRelayFactory
  )
  {
    super(() -> new SelfOnlyDiscovery(discoveryProvider.getForNodeRole(NodeRole.BROKER), selfNode), messageRelayFactory);
  }

  /** A view of a discovery that exposes only the given node. */
  private static class SelfOnlyDiscovery implements DruidNodeDiscovery
  {
    private final DruidNodeDiscovery delegate;
    private final String selfHostAndPort;
    private final Map<Listener, Listener> listeners = new ConcurrentHashMap<>();

    SelfOnlyDiscovery(final DruidNodeDiscovery delegate, final DruidNode selfNode)
    {
      this.delegate = delegate;
      this.selfHostAndPort = selfNode.getHostAndPortToUse();
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return delegate.getAllNodes().stream().filter(this::isSelf).toList();
    }

    @Override
    public void registerListener(final Listener listener)
    {
      final Listener filtered = new Listener()
      {
        @Override
        public void nodesAdded(final Collection<DiscoveryDruidNode> nodes)
        {
          final List<DiscoveryDruidNode> self = nodes.stream().filter(SelfOnlyDiscovery.this::isSelf).toList();
          if (!self.isEmpty()) {
            listener.nodesAdded(self);
          }
        }

        @Override
        public void nodesRemoved(final Collection<DiscoveryDruidNode> nodes)
        {
          final List<DiscoveryDruidNode> self = nodes.stream().filter(SelfOnlyDiscovery.this::isSelf).toList();
          if (!self.isEmpty()) {
            listener.nodesRemoved(self);
          }
        }

        @Override
        public void nodeViewInitialized()
        {
          listener.nodeViewInitialized();
        }
      };
      listeners.put(listener, filtered);
      delegate.registerListener(filtered);
    }

    @Override
    public void removeListener(final Listener listener)
    {
      final Listener filtered = listeners.remove(listener);
      if (filtered != null) {
        delegate.removeListener(filtered);
      }
    }

    private boolean isSelf(final DiscoveryDruidNode node)
    {
      return selfHostAndPort.equals(node.getDruidNode().getHostAndPortToUse());
    }
  }
}
