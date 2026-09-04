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

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ServerSegmentsTableDataProvider;
import org.apache.druid.server.system.table.ServerSegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableQueryRequest;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ServerSegmentsTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", "test", null, null);

  @Test
  public void testReturnsAuthorizedSegmentsInBatchedDataSource()
  {
    final DataSegment allowedSegment = segment("allowed", "2024-01-01/2024-01-02");
    final DataSegment deniedSegment = segment("denied", "2024-01-02/2024-01-03");
    final DruidServer server = new DruidServer(
        "historical",
        "historical:8083",
        null,
        1_000L,
        null,
        ServerType.HISTORICAL,
        "default",
        0
    );
    server.addDataSegment(allowedSegment);
    server.addDataSegment(deniedSegment);

    final TimelineServerView serverView = EasyMock.mock(TimelineServerView.class);
    EasyMock.expect(serverView.getDruidServers()).andReturn(List.of(server.toImmutableDruidServer())).once();
    EasyMock.replay(serverView);

    final ServerSegmentsTableDataProvider provider = new ServerSegmentsTableDataProvider(
        serverView,
        authorizerMapper(true)
    );
    final Iterable<Object[]> authorizedRows = provider.getRows(Collections.emptyList(), AUTHENTICATION_RESULT);
    final RowSignature projectedSignature = RowSignature.builder()
                                                        .add("segment_id", ServerSegmentsTableDescriptor.ROW_SIGNATURE
                                                            .getColumnType("segment_id").orElseThrow())
                                                        .build();
    final DataSource dataSource = provider.getAuthorizedDataSource(
        new SystemTableQueryRequest(List.of("segment_id"), projectedSignature),
        authorizedRows
    ).orElseThrow();

    final BatchedInlineDataSource batchedDataSource = Assertions.assertInstanceOf(
        BatchedInlineDataSource.class,
        dataSource
    );
    final List<Object[]> rows = toRows(batchedDataSource.getRows());
    Assertions.assertEquals(projectedSignature, batchedDataSource.getRowSignature());
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(new Object[]{allowedSegment.getId().toString()}, rows.get(0));
    EasyMock.verify(serverView);
  }

  @Test
  public void testDescriptorRejectsRequestWithoutStateRead()
  {
    final ServerSegmentsTableDescriptor descriptor = new ServerSegmentsTableDescriptor();

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> descriptor.getRowAuthorizer().filterAuthorizedRows(
            Collections.emptyList(),
            AUTHENTICATION_RESULT,
            authorizerMapper(false)
        )
    );
  }

  @Test
  public void testDescriptorRunsLocallyOnBroker()
  {
    final ServerSegmentsTableDescriptor descriptor = new ServerSegmentsTableDescriptor();

    Assertions.assertEquals(Set.of(NodeRole.BROKER), descriptor.getNodeRoles());
    Assertions.assertEquals(SystemTableRoutingMode.LOCAL_ONLY, descriptor.getRoutingMode());
    Assertions.assertEquals(List.of("server", "segment_id"), descriptor.getRowSignature().getColumnNames());
  }

  private static AuthorizerMapper authorizerMapper(final boolean allowState)
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public org.apache.druid.server.security.Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            (allowState && ResourceType.STATE.equals(resource.getType())) || "allowed".equals(resource.getName())
            ? Access.OK
            : Access.DENIED;
      }
    };
  }

  private static DataSegment segment(final String dataSource, final String interval)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(Intervals.of(interval))
                      .version("v1")
                      .size(1L)
                      .build();
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
