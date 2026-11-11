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
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
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

public class TaskTableDescriptorTest
{
  private static final int DATASOURCE_COLUMN = TaskTableDescriptor.ROW_SIGNATURE.indexOf("datasource");
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test", "allow", "test", null);
  private static final AuthorizerMapper AUTHORIZER_MAPPER =
      new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null)));

  @Test
  public void testRowAuthorizerDropsRowsWithoutDatasource()
  {
    final Object[] rowWithoutDatasource = taskRow(null);
    final Object[] authorizedRow = taskRow("wikipedia");

    final List<Object[]> result = Lists.newArrayList(
        new TaskTableDescriptor().getRowAuthorizer()
                                 .authorize(AUTHENTICATION_RESULT, AUTHORIZER_MAPPER)
                                 .filter(List.of(rowWithoutDatasource, authorizedRow))
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertSame(authorizedRow, result.getFirst());
  }

  /**
   * Each row needs DATASOURCE READ on its own datasource, matching the SQL sys.tasks table.
   */
  @Test
  public void testRowAuthorizerKeepsOnlyRowsOfReadableDatasources()
  {
    final Authorizer readsWikipediaOnly = (authenticationResult, resource, action) ->
        ResourceType.DATASOURCE.equals(resource.getType())
        && "wikipedia".equals(resource.getName())
        && Action.READ.equals(action) ? Access.OK : Access.DENIED;
    final Object[] readableRow = taskRow("wikipedia");

    final List<Object[]> result = Lists.newArrayList(
        new TaskTableDescriptor().getRowAuthorizer()
                                 .authorize(
                                     new AuthenticationResult("test", "wikipediaOnly", "test", null),
                                     new AuthorizerMapper(Map.of("wikipediaOnly", readsWikipediaOnly))
                                 )
                                 .filter(List.of(readableRow, taskRow("koalas"), taskRow("wikipedia_extra")))
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertSame(readableRow, result.getFirst());
  }

  /**
   * sys.tasks is read from the leader Overlord only.
   */
  @Test
  public void testReadFromLeaderOverlordOnly()
  {
    Assertions.assertEquals(SystemTableTargetNodes.LEADER_ONLY, new TaskTableDescriptor().getTargetNodes());
    Assertions.assertEquals(Set.of(NodeRole.OVERLORD), new TaskTableDescriptor().getNodeRoles());
  }

  /**
   * Only filters that compare raw values with literals reach the Overlord, which reads rows before they are authorized.
   */
  @Test
  public void testOnlyRowSafeFiltersReachNodeScans()
  {
    Assertions.assertTrue(new TaskTableDescriptor().hasRowLevelAuthorization());
    Assertions.assertTrue(SystemTableAuthorization.isRowSafeFilter(
        new OrDimFilter(
            new EqualityFilter("datasource", ColumnType.STRING, "wikipedia", null),
            new NotDimFilter(new LikeDimFilter("task_id", "index_%", null, null))
        )
    ));
    Assertions.assertFalse(SystemTableAuthorization.isRowSafeFilter(
        new ExpressionDimFilter("strlen(error_msg) > 10", ExprMacroTable.nil())
    ));
    Assertions.assertFalse(SystemTableAuthorization.isRowSafeFilter(
        new SelectorDimFilter("datasource", "WIKIPEDIA", new UpperExtractionFn(null))
    ));
  }

  private static Object[] taskRow(final String dataSource)
  {
    final Object[] row = new Object[TaskTableDescriptor.ROW_SIGNATURE.size()];
    row[DATASOURCE_COLUMN] = dataSource;
    return row;
  }
}
