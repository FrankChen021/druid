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

import com.google.inject.Inject;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Native row supplier for {@code sys.servers}. */
public class ServersTableDataProvider implements SystemTableDataProvider
{
  // This is used for maxSize and currentSize when they are unknown.
  // The unknown size doesn't have to be 0, it's better to be null.
  // However, this table is returning 0 for them for some reason and we keep the behavior for backwards compatibility.
  // Maybe we can remove this and return nulls instead when we remove the bindable query path which is currently
  // used to query system tables.
  private static final long UNKNOWN_SIZE = 0L;

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final FilteredServerInventoryView serverInventoryView;
  private final AuthorizerMapper authorizerMapper;
  private final OverlordClient overlordClient;
  private final CoordinatorClient coordinatorClient;

  @Inject
  public ServersTableDataProvider(
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final FilteredServerInventoryView serverInventoryView,
      final AuthorizerMapper authorizerMapper,
      final OverlordClient overlordClient,
      final CoordinatorClient coordinatorClient
  )
  {
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.serverInventoryView = serverInventoryView;
    this.authorizerMapper = authorizerMapper;
    this.overlordClient = overlordClient;
    this.coordinatorClient = coordinatorClient;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    authorizeServerRead(internalAuthenticationResult);

    final Iterator<DiscoveryDruidNode> druidServers = getDruidServers();
    String tmpCoordinatorLeader = "";
    String tmpOverlordLeader = "";

    try {
      tmpCoordinatorLeader = FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true).toString();
    }
    catch (Exception ignored) {
      // no reason to kill the results if something is sad and there are no leaders
    }

    try {
      tmpOverlordLeader = FutureUtils.getUnchecked(overlordClient.findCurrentLeader(), true).toString();
    }
    catch (Exception ignored) {
      // no reason to kill the results if something is sad and there are no leaders
    }

    final String coordinatorLeader = tmpCoordinatorLeader;
    final String overlordLeader = tmpOverlordLeader;
    final List<Object[]> rows = new ArrayList<>();

    druidServers.forEachRemaining(discoveryDruidNode -> {
      final DataNodeService dataNodeService = discoveryDruidNode.getService(
          DataNodeService.DISCOVERY_SERVICE_KEY,
          DataNodeService.class
      );
      final boolean isDiscoverableDataServer = isDiscoverableDataServer(dataNodeService);
      final NodeRole serverRole = discoveryDruidNode.getNodeRole();

      if (isDiscoverableDataServer) {
        final DruidServer druidServer = serverInventoryView.getInventoryValue(
            discoveryDruidNode.getDruidNode().getHostAndPortToUse()
        );
        if (druidServer != null || NodeRole.HISTORICAL.equals(serverRole)) {
          // Build a row for the data server if that server is in the server view, or the node type is historical.
          // The historicals are usually supposed to be found in the server view. If some historicals are
          // missing, it could mean that there are some problems in them to announce themselves. We just fill
          // their status with nulls in this case.
          rows.add(buildRowForDiscoverableDataServer(discoveryDruidNode, druidServer));
        } else {
          rows.add(buildRowForNonDataServer(discoveryDruidNode));
        }
      } else if (NodeRole.COORDINATOR.equals(serverRole)) {
        rows.add(
            buildRowForNonDataServerWithLeadership(
                discoveryDruidNode,
                coordinatorLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
            )
        );
      } else if (NodeRole.OVERLORD.equals(serverRole)) {
        rows.add(
            buildRowForNonDataServerWithLeadership(
                discoveryDruidNode,
                overlordLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
            )
        );
      } else {
        rows.add(buildRowForNonDataServer(discoveryDruidNode));
      }
    });

    return rows;
  }

  private void authorizeServerRead(final AuthenticationResult authenticationResult)
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(
          "Insufficient permission to view servers: " + authorizationResult.getErrorMessage()
      );
    }
  }

  private Iterator<DiscoveryDruidNode> getDruidServers()
  {
    return Arrays.stream(NodeRole.values())
                 .flatMap(nodeRole -> druidNodeDiscoveryProvider.getForNodeRole(nodeRole).getAllNodes().stream())
                 .iterator();
  }

  private Object[] buildRowForNonDataServer(final DiscoveryDruidNode discoveryDruidNode)
  {
    final DruidNode node = discoveryDruidNode.getDruidNode();
    return new Object[]{
        node.getHostAndPortToUse(),
        node.getHost(),
        (long) node.getPlaintextPort(),
        (long) node.getTlsPort(),
        StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
        null,
        UNKNOWN_SIZE,
        UNKNOWN_SIZE,
        UNKNOWN_SIZE,
        null,
        toStringOrNull(discoveryDruidNode.getStartTime()),
        node.getVersion(),
        node.getBuildRevision(),
        node.getLabels(),
        (long) discoveryDruidNode.getAvailableProcessors(),
        discoveryDruidNode.getTotalMemory()
    };
  }

  private Object[] buildRowForNonDataServerWithLeadership(
      final DiscoveryDruidNode discoveryDruidNode,
      final boolean isLeader
  )
  {
    final DruidNode node = discoveryDruidNode.getDruidNode();
    return new Object[]{
        node.getHostAndPortToUse(),
        node.getHost(),
        (long) node.getPlaintextPort(),
        (long) node.getTlsPort(),
        StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
        null,
        UNKNOWN_SIZE,
        UNKNOWN_SIZE,
        UNKNOWN_SIZE,
        isLeader ? 1L : 0L,
        toStringOrNull(discoveryDruidNode.getStartTime()),
        node.getVersion(),
        node.getBuildRevision(),
        node.getLabels(),
        (long) discoveryDruidNode.getAvailableProcessors(),
        discoveryDruidNode.getTotalMemory()
    };
  }

  private Object[] buildRowForDiscoverableDataServer(
      final DiscoveryDruidNode discoveryDruidNode,
      @Nullable final DruidServer serverFromInventoryView
  )
  {
    final DruidNode node = discoveryDruidNode.getDruidNode();
    final DruidServer druidServerToUse = serverFromInventoryView == null
                                         ? toDruidServer(discoveryDruidNode)
                                         : serverFromInventoryView;
    final long currentSize = serverFromInventoryView == null
                             ? UNKNOWN_SIZE
                             : serverFromInventoryView.getCurrSize();
    return new Object[]{
        node.getHostAndPortToUse(),
        node.getHost(),
        (long) node.getPlaintextPort(),
        (long) node.getTlsPort(),
        StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
        druidServerToUse.getTier(),
        currentSize,
        druidServerToUse.getMaxSize(),
        druidServerToUse.getStorageSize(),
        null,
        toStringOrNull(discoveryDruidNode.getStartTime()),
        node.getVersion(),
        node.getBuildRevision(),
        node.getLabels(),
        (long) discoveryDruidNode.getAvailableProcessors(),
        discoveryDruidNode.getTotalMemory()
    };
  }

  private static boolean isDiscoverableDataServer(@Nullable final DataNodeService dataNodeService)
  {
    return dataNodeService != null && dataNodeService.isDiscoverable();
  }

  private static DruidServer toDruidServer(final DiscoveryDruidNode discoveryDruidNode)
  {
    final DruidNode druidNode = discoveryDruidNode.getDruidNode();
    final DataNodeService dataNodeService = discoveryDruidNode.getService(
        DataNodeService.DISCOVERY_SERVICE_KEY,
        DataNodeService.class
    );
    if (isDiscoverableDataServer(dataNodeService)) {
      return new DruidServer(
          druidNode.getHostAndPortToUse(),
          druidNode.getHostAndPort(),
          druidNode.getHostAndTlsPort(),
          dataNodeService.getMaxSize(),
          dataNodeService.getStorageSize(),
          dataNodeService.getServerType(),
          dataNodeService.getTier(),
          dataNodeService.getPriority()
      );
    } else {
      throw new ISE("[%s] is not a discoverable data server", discoveryDruidNode);
    }
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    return object == null ? null : object.toString();
  }
}
