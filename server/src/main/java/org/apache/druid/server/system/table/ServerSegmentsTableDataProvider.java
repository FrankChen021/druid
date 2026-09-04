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

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Native row supplier for {@code sys.server_segments}. */
public class ServerSegmentsTableDataProvider implements SystemTableDataProvider
{
  private final TimelineServerView serverView;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public ServerSegmentsTableDataProvider(
      final TimelineServerView serverView,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.serverView = serverView;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public Optional<DataSource> getAuthorizedDataSource(
      final SystemTableQueryRequest request,
      final Iterable<Object[]> authorizedRows
  )
  {
    final int[] projects = request.columns()
                                  .stream()
                                  .mapToInt(ServerSegmentsTableDescriptor.ROW_SIGNATURE::indexOf)
                                  .toArray();
    return Optional.of(
        new BatchedInlineDataSource(
            Iterables.transform(authorizedRows, row -> projectRow(row, projects)),
            request.rowSignature()
        )
    );
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult authenticationResult
  )
  {
    final Iterable<Iterable<Object[]>> rowsByServer = Iterables.transform(
        serverView.getDruidServers(),
        server -> getAuthorizedRows(server, authenticationResult)
    );
    return Iterables.concat(rowsByServer);
  }

  private Iterable<Object[]> getAuthorizedRows(
      final ImmutableDruidServer server,
      final AuthenticationResult authenticationResult
  )
  {
    final Iterable<DataSegment> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        server.iterateAllSegments(),
        segment -> Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSource())
        ),
        authorizerMapper
    );
    return Iterables.transform(
        authorizedSegments,
        segment -> new Object[]{server.getHost(), segment.getId().toString()}
    );
  }
}
