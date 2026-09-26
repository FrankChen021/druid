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
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
        new TaskTableDescriptor().getRowAuthorizer().filterAuthorizedRows(
            List.of(rowWithoutDatasource, authorizedRow),
            AUTHENTICATION_RESULT,
            AUTHORIZER_MAPPER
        )
    );

    Assertions.assertEquals(1, result.size());
    Assertions.assertSame(authorizedRow, result.getFirst());
  }

  private static Object[] taskRow(final String dataSource)
  {
    final Object[] row = new Object[TaskTableDescriptor.ROW_SIGNATURE.size()];
    row[DATASOURCE_COLUMN] = dataSource;
    return row;
  }
}
