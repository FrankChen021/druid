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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.input.system.SystemTableSource;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.handler.SystemTableNode;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DartSystemTableInputSpecSlicerTest
{
  private static final DruidNode BROKER = node("broker", 8082);
  private static final DruidNode HISTORICAL = node("historical", 8083);
  private static final DruidNode COORDINATOR = node("coordinator", 8081);
  private static final String QUERY_ID = "query-id";
  private static final SystemTableDescriptor DESCRIPTOR = new ServerPropertiesTableDescriptor();

  /** A Historical source is assigned locally, while the Broker proxies a control-plane source. */
  @Test
  public void test_sliceStatic_assignsLocalWorkersAndBrokerFallback()
  {
    final SystemTableNodeLocator locator = locator(
        List.of(
            located(HISTORICAL, NodeRole.HISTORICAL),
            located(COORDINATOR, NodeRole.COORDINATOR),
            located(BROKER, NodeRole.BROKER)
        )
    );
    final DartSystemTableInputSpecSlicer slicer = new DartSystemTableInputSpecSlicer(
        locator,
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, DESCRIPTOR),
        List.of(worker(BROKER), worker(HISTORICAL)),
        Long.MAX_VALUE
    );

    final List<InputSlice> slices = slicer.sliceStatic(
        new SystemTableInputSpec(ServerPropertiesTableDescriptor.TABLE_NAME),
        null,
        2
    );

    Assertions.assertEquals(
        List.of(
            new SystemTableInputSlice(
                ServerPropertiesTableDescriptor.TABLE_NAME,
                List.of(
                    new SystemTableSource(COORDINATOR, Set.of(NodeRole.COORDINATOR)),
                    new SystemTableSource(BROKER, Set.of(NodeRole.BROKER))
                ),
                null,
                null,
                Long.MAX_VALUE
            ),
            new SystemTableInputSlice(
                ServerPropertiesTableDescriptor.TABLE_NAME,
                List.of(new SystemTableSource(HISTORICAL, Set.of(NodeRole.HISTORICAL))),
                null,
                null,
                Long.MAX_VALUE
            )
        ),
        slices
    );
  }

  /** Workers without an assigned source receive a nil slice. */
  @Test
  public void test_sliceStatic_unusedWorkerGetsNilSlice()
  {
    final DartSystemTableInputSpecSlicer slicer = new DartSystemTableInputSpecSlicer(
        locator(List.of(located(BROKER, NodeRole.BROKER))),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, DESCRIPTOR),
        List.of(worker(BROKER), worker(HISTORICAL)),
        Long.MAX_VALUE
    );

    final List<InputSlice> slices = slicer.sliceStatic(
        new SystemTableInputSpec(ServerPropertiesTableDescriptor.TABLE_NAME),
        null,
        2
    );
    Assertions.assertInstanceOf(SystemTableInputSlice.class, slices.get(0));
    Assertions.assertSame(NilInputSlice.INSTANCE, slices.get(1));
  }

  private static SystemTableNodeLocator locator(final List<SystemTableNode> nodes)
  {
    final SystemTableNodeLocator locator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(locator.locate(Mockito.same(DESCRIPTOR), Mockito.anyLong())).thenReturn(nodes);
    return locator;
  }

  private static SystemTableNode located(final DruidNode node, final NodeRole role)
  {
    final SystemTableNode located = Mockito.mock(SystemTableNode.class);
    Mockito.when(located.getDiscoveryNode()).thenReturn(new DiscoveryDruidNode(node, role, Map.of()));
    Mockito.when(located.getNodeRoles()).thenReturn(Set.of(role));
    return located;
  }

  private static String worker(final DruidNode node)
  {
    return WorkerId.fromDruidNode(node, QUERY_ID).toString();
  }

  private static DruidNode node(final String service, final int port)
  {
    return new DruidNode(service, "localhost", false, port, -1, true, false);
  }
}
