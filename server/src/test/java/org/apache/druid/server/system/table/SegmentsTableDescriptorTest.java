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

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class SegmentsTableDescriptorTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  @Test
  public void testDescriptorDefinesBrokerLocalSegmentsTable()
  {
    final SegmentsTableDescriptor descriptor = new SegmentsTableDescriptor();

    Assertions.assertEquals("segments", descriptor.getTableName());
    Assertions.assertEquals(Set.of(NodeRole.BROKER), descriptor.getNodeRoles());
    Assertions.assertEquals(SystemTableRoutingMode.LOCAL_ONLY, descriptor.getRoutingMode());
    Assertions.assertEquals(20, descriptor.getRowSignature().size());
    Assertions.assertEquals("datasource", descriptor.getRowSignature().getColumnNames().get(1));
  }

  @Test
  public void testDescriptorFiltersRowsByDatasourceReadPermission()
  {
    final DataSegment allowedSegment = DataSegment.builder(
        SegmentId.of("allowed", Intervals.of("2000/2001"), "v", null)
    ).build();
    final DataSegment deniedSegment = DataSegment.builder(
        SegmentId.of("denied", Intervals.of("2000/2001"), "v", null)
    ).build();
    final List<Object[]> rows = List.of(
        row(allowedSegment.getDataSource()),
        row(deniedSegment.getDataSource())
    );
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            "allowed".equals(resource.getName()) ? Access.OK : Access.DENIED;
      }
    };

    final List<Object[]> authorizedRows = toList(
        new SegmentsTableDescriptor().getRowAuthorizer().filterAuthorizedRows(
            rows,
            AUTHENTICATION_RESULT,
            authorizerMapper
        )
    );

    Assertions.assertEquals(1, authorizedRows.size());
    Assertions.assertEquals("allowed", authorizedRows.get(0)[1]);
  }

  private static Object[] row(final String datasource)
  {
    final Object[] row = new Object[SegmentsTableDescriptor.ROW_SIGNATURE.size()];
    row[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = datasource;
    return row;
  }

  private static List<Object[]> toList(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new java.util.ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
