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
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ServerSegmentsTableDataProvider;
import org.apache.druid.server.system.table.ServerSegmentsTableDescriptor;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ServerSegmentsTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", "test", null, null);

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
                new SelectorFilter("server", "selected:8083"),
                new EqualityFilter("segment_id", ColumnType.STRING, selectedSegment.getId().toString(), null),
                new InDimFilter("datasource", Set.of("selected", "missing"))
            )
        )
    );

    Assertions.assertEquals(Set.of("server", "segment_id", "datasource"), provider.getPushdownColumns());
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
        () -> descriptor.getRowAuthorizer().authorize(AUTHENTICATION_RESULT, denyStateAuthorizerMapper())
    );
  }

  @Test
  public void testDescriptorKeepsRowsOfReadableDatasources()
  {
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public org.apache.druid.server.security.Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            ResourceType.DATASOURCE.equals(resource.getType()) && !"allowed".equals(resource.getName())
            ? Access.DENIED
            : Access.OK;
      }
    };

    final List<Object[]> rows = toRows(
        new ServerSegmentsTableDescriptor().getRowAuthorizer().authorize(AUTHENTICATION_RESULT, authorizerMapper).filter(
            List.of(
                new Object[]{"historical:8083", "allowed_segment", "allowed"},
                new Object[]{"historical:8083", "denied_segment", "denied"}
            )
        )
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("allowed", rows.get(0)[2]);
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
    return DataSegment.builder(SegmentId.of(dataSource, Intervals.of(interval), "v1", null)).size(1L).build();
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
