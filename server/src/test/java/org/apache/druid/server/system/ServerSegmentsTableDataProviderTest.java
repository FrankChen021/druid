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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.SelectorDimFilter;
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
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServerSegmentsTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", "test", null, null);

  @Test
  public void testProjectsRowsIntoBatchedDataSource()
  {
    final DataSegment segment = segment("foo", "2024-01-01/2024-01-02");
    final ServerSegmentsTableDataProvider provider = new ServerSegmentsTableDataProvider(
        EasyMock.mock(TimelineServerView.class)
    );
    final RowSignature projectedSignature = RowSignature.builder()
                                                        .add("segment_id", ServerSegmentsTableDescriptor.ROW_SIGNATURE
                                                            .getColumnType("segment_id").orElseThrow())
                                                        .build();
    final DataSource dataSource = provider.getAuthorizedDataSource(
        new SystemTableQueryRequest(List.of("segment_id"), projectedSignature),
        List.<Object[]>of(new Object[]{"historical:8083", segment.getId().toString(), "foo"})
    ).orElseThrow();

    final BatchedInlineDataSource batchedDataSource = Assertions.assertInstanceOf(
        BatchedInlineDataSource.class,
        dataSource
    );
    final List<Object[]> rows = toRows(batchedDataSource.getRows());
    Assertions.assertEquals(projectedSignature, batchedDataSource.getRowSignature());
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(new Object[]{segment.getId().toString()}, rows.get(0));
  }

  @Test
  public void testPushesDownServerSegmentAndDatasourceFilters()
  {
    final DataSegment selectedSegment = segment("selected", "2024-01-01/2024-01-02");
    final DataSegment otherSegment = segment("other", "2024-01-02/2024-01-03");
    final DruidServer selectedServer = server("selected:8083", selectedSegment, otherSegment);
    final DruidServer otherServer = server("other:8083", selectedSegment);
    final TimelineServerView serverView = EasyMock.mock(TimelineServerView.class);
    EasyMock.expect(serverView.getDruidServers()).andReturn(
        List.of(selectedServer.toImmutableDruidServer(), otherServer.toImmutableDruidServer())
    ).once();
    EasyMock.replay(serverView);

    final ServerSegmentsTableDataProvider provider = new ServerSegmentsTableDataProvider(serverView);
    final List<Object[]> rows = toRows(
        provider.getRows(
            List.of(
                new SelectorDimFilter("server", "selected:8083", null),
                new SelectorDimFilter("segment_id", selectedSegment.getId().toString(), null),
                new SelectorDimFilter("datasource", "selected", null)
            )
        )
    );

    Assertions.assertEquals(
        List.of("server", "segment_id", "datasource"),
        provider.getPushdownFilters().stream().map(filter -> filter.key()).toList()
    );
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(
        new Object[]{"selected:8083", selectedSegment.getId().toString(), "selected"},
        rows.get(0)
    );
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
            denyStateAuthorizerMapper()
        )
    );
  }

  private static AuthorizerMapper denyStateAuthorizerMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public org.apache.druid.server.security.Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            ResourceType.STATE.equals(resource.getType()) ? Access.DENIED : Access.OK;
      }
    };
  }

  private static DruidServer server(final String host, final DataSegment... segments)
  {
    final DruidServer server = new DruidServer(
        host,
        host,
        null,
        1_000L,
        null,
        ServerType.HISTORICAL,
        "default",
        0
    );
    for (final DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return server;
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
