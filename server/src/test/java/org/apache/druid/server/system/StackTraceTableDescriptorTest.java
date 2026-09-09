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

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.Druids;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.StackTraceTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class StackTraceTableDescriptorTest
{
  private final SystemTableDescriptor descriptor = new StackTraceTableDescriptor();

  @Test
  public void testDescribesStackTraceRows()
  {
    Assertions.assertEquals("stack_trace", descriptor.getTableName());
    Assertions.assertEquals(
        List.of(
            "server",
            "service_name",
            "node_roles",
            "collected_at",
            "thread_id",
            "thread_name",
            "thread_state",
            "daemon",
            "priority",
            "cpu_time_ns",
            "user_cpu_time_ns",
            "lock_name",
            "lock_owner_id",
            "lock_owner_name",
            "is_deadlocked",
            "stack",
            "error_message"
        ),
        descriptor.getRowSignature().getColumnNames()
    );
    Assertions.assertEquals(
        ColumnType.LONG,
        descriptor.getRowSignature().getColumnType("thread_id").orElseThrow()
    );
    Assertions.assertEquals(Set.of(NodeRole.values()), descriptor.getNodeRoles());
    Assertions.assertTrue(descriptor.isEmptyDiscoveryAllowed());
  }

  @Test
  public void testRequiresExactServerFilter()
  {
    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> descriptor.validateQuery(query(null))
    );
    Assertions.assertTrue(exception.getMessage().contains("requires a filter on the server column"));

    Assertions.assertThrows(
        DruidException.class,
        () -> descriptor.validateQuery(query(new LikeDimFilter("server", "%localhost%", null, null)))
    );
    Assertions.assertThrows(
        DruidException.class,
        () -> descriptor.validateQuery(query(new SelectorDimFilter("service_name", "broker", null)))
    );
  }

  @Test
  public void testAcceptsEqualityAndInServerFilters()
  {
    descriptor.validateQuery(query(new SelectorDimFilter("server", "localhost:8080", null)));
    descriptor.validateQuery(query(new InDimFilter("server", List.of("localhost:8080", "localhost:8081"), null)));
  }

  @Test
  public void testBuildsNodeFailureRow()
  {
    final Object[] row = descriptor.getNodeFailureRow(
        new DruidNode("coordinator", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.COORDINATOR, NodeRole.OVERLORD),
        new IllegalStateException("connection failed")
    ).orElseThrow();

    Assertions.assertArrayEquals(
        new Object[]{
            "localhost:8080",
            "coordinator",
            "coordinator,overlord",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "connection failed"
        },
        row
    );
  }

  private static ScanQuery query(final org.apache.druid.query.filter.DimFilter filter)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource(StackTraceTableDescriptor.TABLE_NAME))
                 .eternityInterval()
                 .filters(filter)
                 .build();
  }
}
