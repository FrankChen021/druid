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

import com.google.common.collect.Lists;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.extraction.UpperExtractionFn;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SupervisorTableDescriptorTest
{
  private static final int SUPERVISOR_ID_COLUMN = SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("supervisor_id");
  private static final int DATASOURCE_COLUMN = SupervisorTableDescriptor.ROW_SIGNATURE.indexOf("datasource");
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test", "allow", "test", null);
  private static final AuthorizerMapper AUTHORIZER_MAPPER =
      new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null)));

  /**
   * The table has the columns of the SQL sys.supervisors table and no others, so no column used only for authorization
   * can reach query results.
   */
  @Test
  public void testRowSignatureHasOnlyPublicColumns()
  {
    Assertions.assertEquals(
        List.of(
            "supervisor_id",
            "datasource",
            "state",
            "detailed_state",
            "healthy",
            "type",
            "source",
            "suspended",
            "spec"
        ),
        SupervisorTableDescriptor.ROW_SIGNATURE.getColumnNames()
    );
  }

  /**
   * A supervisor whose spec lists no datasource is hidden even from a caller who may read every datasource, since no
   * permission can be checked for it.
   */
  @Test
  public void testRowAuthorizerDropsRowsWithoutDatasource()
  {
    final Object[] rowWithoutDatasource = supervisorRow("supervisor-none", null);
    final Object[] authorizedRow = supervisorRow("supervisor-wikipedia", "wikipedia");

    final List<Object[]> result = Lists.newArrayList(
        new SupervisorTableDescriptor().getRowAuthorizer()
                                       .authorize(AUTHENTICATION_RESULT, AUTHORIZER_MAPPER)
                                       .filter(List.of(rowWithoutDatasource, authorizedRow))
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertSame(authorizedRow, result.getFirst());
  }

  /**
   * Each row needs DATASOURCE READ on its own datasource, matching the SQL sys.supervisors table.
   */
  @Test
  public void testRowAuthorizerKeepsOnlyRowsOfReadableDatasources()
  {
    final Authorizer readsWikipediaOnly = (authenticationResult, resource, action) ->
        ResourceType.DATASOURCE.equals(resource.getType())
        && "wikipedia".equals(resource.getName())
        && Action.READ.equals(action) ? Access.OK : Access.DENIED;
    final Object[] readableRow = supervisorRow("supervisor-wikipedia", "wikipedia");

    final List<Object[]> result = Lists.newArrayList(
        new SupervisorTableDescriptor().getRowAuthorizer()
                                       .authorize(
                                           new AuthenticationResult("test", "wikipediaOnly", "test", null),
                                           new AuthorizerMapper(Map.of("wikipediaOnly", readsWikipediaOnly))
                                       )
                                       .filter(
                                           List.of(
                                               readableRow,
                                               supervisorRow("supervisor-koalas", "koalas"),
                                               supervisorRow("supervisor-extra", "wikipedia_extra")
                                           )
                                       )
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertSame(readableRow, result.getFirst());
  }

  /**
   * sys.supervisors is read from the leader Overlord only, and its rows are authorized one by one.
   */
  @Test
  public void testReadFromLeaderOverlordOnly()
  {
    final SupervisorTableDescriptor descriptor = new SupervisorTableDescriptor();
    Assertions.assertEquals(SystemTableTargetNodes.LEADER_ONLY, descriptor.getTargetNodes());
    Assertions.assertEquals(Set.of(NodeRole.OVERLORD), descriptor.getNodeRoles());
    Assertions.assertTrue(descriptor.hasRowLevelAuthorization());
  }

  /**
   * Only filters that compare raw values with literals are row-safe, so only they reach the Overlord, which reads rows
   * before they are authorized.
   */
  @Test
  public void testRowSafeFilters()
  {
    Assertions.assertTrue(SystemTableAuthorization.isRowSafeFilter(
        new InDimFilter("supervisor_id", Set.of("supervisor-a", "supervisor-b"))
    ));
    Assertions.assertTrue(SystemTableAuthorization.isRowSafeFilter(
        new EqualityFilter("datasource", ColumnType.STRING, "wikipedia", null)
    ));
    Assertions.assertFalse(SystemTableAuthorization.isRowSafeFilter(
        new ExpressionDimFilter("strlen(spec) > 10", ExprMacroTable.nil())
    ));
    Assertions.assertFalse(SystemTableAuthorization.isRowSafeFilter(
        new SelectorDimFilter("supervisor_id", "SUPERVISOR-A", new UpperExtractionFn(null))
    ));
  }

  private static Object[] supervisorRow(final String supervisorId, final String dataSource)
  {
    final Object[] row = new Object[SupervisorTableDescriptor.ROW_SIGNATURE.size()];
    row[SUPERVISOR_ID_COLUMN] = supervisorId;
    row[DATASOURCE_COLUMN] = dataSource;
    return row;
  }
}
