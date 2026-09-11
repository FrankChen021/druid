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
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Contributes native query executions registered with the local {@link QueryScheduler}. */
public class QuerySchedulerQueryInfoProvider implements SystemTableQueryInfoProvider
{
  private static final String NATIVE_ENGINE = "native";
  private static final String RUNNING_STATE = "RUNNING";

  private final QueryScheduler queryScheduler;
  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;

  @Inject
  public QuerySchedulerQueryInfoProvider(
      final QueryScheduler queryScheduler,
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles
  )
  {
    this.queryScheduler = queryScheduler;
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
  }

  @Override
  public Iterable<SystemTableQueryInfo> getQueryInfo()
  {
    final List<SystemTableQueryInfo> queryInfo = new ArrayList<>();
    for (final QueryScheduler.RegisteredQueryInfo registeredQuery : queryScheduler.getRunningQueryInfo()) {
      if (registeredQuery.id().startsWith(SystemTableDataSource.NODE_QUERY_ID_PREFIX)) {
        continue;
      }

      final String initialQueryId = registeredQuery.sqlQueryId() == null
                                    ? registeredQuery.id()
                                    : registeredQuery.sqlQueryId();
      final boolean isInitialQuery = selfNodeRoles.contains(NodeRole.BROKER)
                                     && registeredQuery.subQueryId() == null
                                     && registeredQuery.dartQueryId() == null;
      queryInfo.add(
          new SystemTableQueryInfo(
              registeredQuery.id(),
              NATIVE_ENGINE,
              RUNNING_STATE,
              makeInfo(registeredQuery),
              initialQueryId,
              isInitialQuery,
              selfNode.getHostAndPortToUse(),
              selfNodeRoles.stream()
                           .map(NodeRole::getJsonName)
                           .sorted()
                           .collect(Collectors.joining(","))
          )
      );
    }
    return queryInfo;
  }

  private static StructuredData makeInfo(final QueryScheduler.RegisteredQueryInfo queryInfo)
  {
    final Map<String, Object> info = new LinkedHashMap<>();
    info.put("queryType", queryInfo.queryType());
    info.put("datasources", queryInfo.datasources());
    if (queryInfo.sqlQueryId() != null) {
      info.put("sqlQueryId", queryInfo.sqlQueryId());
    }
    if (queryInfo.subQueryId() != null) {
      info.put("subQueryId", queryInfo.subQueryId());
    }
    if (queryInfo.dartQueryId() != null) {
      info.put("dartQueryId", queryInfo.dartQueryId());
    }
    return StructuredData.wrap(info);
  }
}
