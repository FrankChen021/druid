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
import org.apache.druid.server.system.table.ServersTableDescriptor;
import org.apache.druid.server.system.table.SystemTableAuthorization;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * The rows of {@code sys.servers} are built by {@code ServersTableDataProvider} for both the native and the SQL path,
 * and are covered by {@code SystemSchemaTest#testServersTable}.
 */
public class ServersTableDescriptorTest
{
  /** Served by the Broker itself, and authorized with STATE READ, as the SQL table is. */
  @Test
  public void testDescriptor()
  {
    final ServersTableDescriptor descriptor = new ServersTableDescriptor();

    Assertions.assertEquals(Set.of(NodeRole.BROKER), descriptor.getNodeRoles());
    Assertions.assertEquals(SystemTableRoutingMode.LOCAL, descriptor.getRoutingMode());
    Assertions.assertSame(SystemTableAuthorization.STATE_READ_ROW_AUTHORIZER, descriptor.getRowAuthorizer());
  }
}
