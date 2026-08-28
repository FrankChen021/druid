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
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SupervisorTableDescriptorTest
{
  @Test
  public void testDescriptorMetadata()
  {
    final SupervisorTableDescriptor descriptor = new SupervisorTableDescriptor();

    Assertions.assertEquals("supervisors", descriptor.getTableName());
    Assertions.assertEquals(Set.of(NodeRole.OVERLORD), descriptor.getNodeRoles());
    Assertions.assertEquals(SystemTableRoutingMode.LEADER_ONLY, descriptor.getRoutingMode());
    Assertions.assertEquals(SupervisorTableDescriptor.ROW_SIGNATURE, descriptor.getRowSignature());
  }

  @Test
  public void testAuthorizesRowsByDatasource()
  {
    final SupervisorTableDescriptor descriptor = new SupervisorTableDescriptor();
    final AuthenticationResult authenticationResult = new AuthenticationResult("user", "test", null, null);
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        Map.of(
            "test",
            (authentication, resource, action) -> "datasource-a".equals(resource.getName())
                                                   ? Access.OK
                                                   : Access.deny("denied")
        )
    );
    final Object[] authorizedRow = row("supervisor-a", "datasource-a");
    final Object[] deniedRow = row("supervisor-b", "datasource-b");

    final List<Object[]> filteredRows = new ArrayList<>();
    descriptor.getRowAuthorizer()
              .filterAuthorizedRows(List.of(authorizedRow, deniedRow), authenticationResult, authorizerMapper)
              .forEach(filteredRows::add);

    Assertions.assertEquals(1, filteredRows.size());
    Assertions.assertSame(authorizedRow, filteredRows.get(0));
  }

  private static Object[] row(final String supervisorId, final String datasource)
  {
    final Object[] row = new Object[SupervisorTableDescriptor.ROW_SIGNATURE.size()];
    row[SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("supervisor_id")] = supervisorId;
    row[SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = datasource;
    return row;
  }
}
