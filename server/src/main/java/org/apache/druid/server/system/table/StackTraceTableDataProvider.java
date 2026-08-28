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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.StackTraceCollector;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Native row supplier for {@code sys.stack_trace}. */
public class StackTraceTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", null),
      new SystemTablePushdownFilter("service_name", null)
  );

  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public StackTraceTableDataProvider(
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    return getRows(filters, internalAuthenticationResult, Collections.emptyMap());
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult,
      final Map<String, Object> queryContext
  )
  {
    authorizeStackTraceRead(internalAuthenticationResult);

    final String server = selfNode.getHostAndPortToUse();
    if (!matchesNode(filters, "server", server)
        || !matchesNode(filters, "service_name", selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final int maxStackTraceFrameDepth = StackTraceCollector.getMaxStackTraceFrameDepth(
        queryContext.get(StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY)
    );
    final StackTraceCollector.ThreadStackTraceResponse response = new StackTraceCollector().collect(
        maxStackTraceFrameDepth
    );
    final String nodeRoles = selfNodeRoles.stream()
                                          .map(NodeRole::getJsonName)
                                          .sorted()
                                          .collect(Collectors.joining(","));

    return response.getThreads()
                   .stream()
                   .map(thread -> new Object[]{
                       server,
                       selfNode.getServiceName(),
                       nodeRoles,
                       response.getCollectedAt(),
                       thread.getThreadId(),
                       thread.getThreadName(),
                       thread.getThreadState(),
                       thread.isDaemon() ? 1L : 0L,
                       (long) thread.getPriority(),
                       thread.getCpuTimeNs(),
                       thread.getUserCpuTimeNs(),
                       thread.getLockName(),
                       thread.getLockOwnerId(),
                       thread.getLockOwnerName(),
                       thread.isDeadlocked() ? 1L : 0L,
                       thread.getStackTrace(),
                       null
                   })
                   .collect(Collectors.toList());
  }

  private void authorizeStackTraceRead(final AuthenticationResult authenticationResult)
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(
          "Insufficient permission to view stack traces: " + authorizationResult.getErrorMessage()
      );
    }
  }

  private static boolean matchesNode(
      final List<DimFilter> filters,
      final String column,
      final String value
  )
  {
    return filters.stream()
                  .filter(StackTraceTableDataProvider::isStringValuesFilter)
                  .filter(filter -> column.equals(SystemTablePushdownFilter.getStringValuesColumn(filter)))
                  .allMatch(filter -> SystemTablePushdownFilter.getStringValues(filter).contains(value));
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
  }
}
