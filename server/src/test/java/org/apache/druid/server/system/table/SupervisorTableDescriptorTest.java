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

import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SupervisorTableDescriptorTest
{
  @Test
  public void testAuthorizesRowsByDatasource()
  {
    final SupervisorTableDescriptor descriptor = new SupervisorTableDescriptor();
    Assertions.assertEquals(
        SupervisorTableDescriptor.AUTHORIZATION_DATASOURCES_COLUMN,
        descriptor.getTransportRowSignature().getColumnName(SupervisorTableDescriptor.ROW_SIGNATURE.size())
    );
    final Object[] authorizedRow = row("supervisor-a", "datasource-a", new Object[]{"datasource-a"});
    final Object[] deniedRow = row("supervisor-b", "datasource-b", List.of("datasource-b"));
    final Object[] partlyAuthorizedRow = row(
        "supervisor-multi",
        "datasource-a",
        List.of("datasource-a", "datasource-b")
    );
    final Object[] missingSpecRow = row("supervisor-missing", null, null);

    final List<Object[]> filteredRows = filterAuthorizedRows(
        descriptor,
        List.of(authorizedRow, deniedRow, partlyAuthorizedRow, missingSpecRow),
        authorizeOnly("datasource-a")
    );

    Assertions.assertEquals(1, filteredRows.size());
    Assertions.assertSame(authorizedRow, filteredRows.get(0));
    Assertions.assertArrayEquals(
        new Object[]{"supervisor-a", "datasource-a", null, null, null, null, null, null, null},
        descriptor.toPublicRow(authorizedRow)
    );
  }

  /**
   * A supervisor whose spec lists no datasources is denied even to a user who may read every datasource. The Broker
   * reads the column from the Overlord's JSON response, where the empty list may arrive as an empty array or as null.
   */
  @Test
  public void testDeniesRowsWithoutDatasources()
  {
    final SupervisorTableDescriptor descriptor = new SupervisorTableDescriptor();
    final AuthorizerMapper allowAll = new AuthorizerMapper(
        Map.of("test", (authentication, resource, action) -> Access.OK)
    );

    Assertions.assertEquals(
        List.of(),
        filterAuthorizedRows(
            descriptor,
            List.of(
                row("supervisor-empty-list", null, List.of()),
                row("supervisor-empty-array", null, new Object[0]),
                row("supervisor-null", null, null)
            ),
            allowAll
        )
    );
  }

  private static List<Object[]> filterAuthorizedRows(
      final SupervisorTableDescriptor descriptor,
      final List<Object[]> rows,
      final AuthorizerMapper authorizerMapper
  )
  {
    final List<Object[]> filteredRows = new ArrayList<>();
    descriptor.getRowAuthorizer()
              .filterAuthorizedRows(rows, new AuthenticationResult("user", "test", null, null), authorizerMapper)
              .forEach(filteredRows::add);
    return filteredRows;
  }

  private static AuthorizerMapper authorizeOnly(final String datasource)
  {
    return new AuthorizerMapper(
        Map.of(
            "test",
            (authentication, resource, action) -> datasource.equals(resource.getName())
                                                   ? Access.OK
                                                   : Access.deny("denied")
        )
    );
  }

  private static Object[] row(
      final String supervisorId,
      final String datasource,
      final Object authorizationDatasources
  )
  {
    final Object[] row = new Object[SupervisorTableDescriptor.ROW_SIGNATURE.size() + 1];
    row[SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("supervisor_id")] = supervisorId;
    row[SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = datasource;
    row[SupervisorTableDescriptor.ROW_SIGNATURE.size()] = authorizationDatasources;
    return row;
  }
}
