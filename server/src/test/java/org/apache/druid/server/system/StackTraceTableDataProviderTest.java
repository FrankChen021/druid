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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.system.table.StackTraceTableDataProvider;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StackTraceTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  @Test
  public void testAdvertisesServerAndServiceNamePushdownFilters()
  {
    final StackTraceTableDataProvider provider = provider(allowAllAuthorizerMapper());

    Assertions.assertEquals(
        List.of(
            new SystemTablePushdownFilter("server", null),
            new SystemTablePushdownFilter("service_name", null)
        ),
        provider.getPushdownFilters()
    );
  }

  @Test
  public void testReturnsStackRowsWithNodeMetadata()
  {
    final StackTraceTableDataProvider provider = provider(allowAllAuthorizerMapper());
    final List<Object[]> rows = toRows(provider.getRows(Collections.emptyList(), AUTHENTICATION_RESULT));

    Assertions.assertFalse(rows.isEmpty());
    final Object[] row = rows.get(0);
    Assertions.assertEquals("localhost:8080", row[0]);
    Assertions.assertEquals("coordinator", row[1]);
    Assertions.assertEquals("coordinator,overlord", row[2]);
    Assertions.assertNotNull(row[3]);
    Assertions.assertInstanceOf(Long.class, row[4]);
    Assertions.assertInstanceOf(String.class, row[5]);
    Assertions.assertInstanceOf(String.class, row[15]);
    Assertions.assertTrue(((String) row[15]).contains("\n\tat "));
    Assertions.assertNull(row[16]);
  }

  @Test
  public void testAppliesServerServiceNameAndInFilters()
  {
    final StackTraceTableDataProvider provider = provider(allowAllAuthorizerMapper());
    final DimFilter wrongServer = new SelectorDimFilter("server", "other:8080", null);
    final DimFilter wrongService = new SelectorDimFilter("service_name", "broker", null);

    Assertions.assertTrue(toRows(provider.getRows(List.of(wrongServer), AUTHENTICATION_RESULT)).isEmpty());
    Assertions.assertTrue(toRows(provider.getRows(List.of(wrongService), AUTHENTICATION_RESULT)).isEmpty());
    Assertions.assertFalse(
        toRows(
            provider.getRows(
                List.of(new InDimFilter("server", List.of("other:8080", "localhost:8080"), null)),
                AUTHENTICATION_RESULT
            )
        ).isEmpty()
    );
  }

  @Test
  public void testUsesMaxStackTraceFrameDepthFromQueryContext()
  {
    final StackTraceTableDataProvider provider = provider(allowAllAuthorizerMapper());
    final List<Object[]> rows = toRows(
        provider.getRows(
            Collections.emptyList(),
            AUTHENTICATION_RESULT,
            Map.of("maxStackTraceFrameDepth", 10.9)
        )
    );

    Assertions.assertFalse(rows.isEmpty());
    Assertions.assertTrue(
        rows.stream().allMatch(row -> countStackFrames((String) row[15]) <= 10)
    );
  }

  @Test
  public void testRejectsInvalidMaxStackTraceFrameDepth()
  {
    final StackTraceTableDataProvider provider = provider(allowAllAuthorizerMapper());

    Assertions.assertThrows(
        DruidException.class,
        () -> provider.getRows(
            Collections.emptyList(),
            AUTHENTICATION_RESULT,
            Map.of("maxStackTraceFrameDepth", 9)
        )
    );
  }

  @Test
  public void testRejectsUnauthorizedRequest()
  {
    final Authorizer denyAll = (authenticationResult, resource, action) -> Access.DENIED;
    final StackTraceTableDataProvider provider = provider(new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return denyAll;
      }
    });

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> provider.getRows(Collections.emptyList(), AUTHENTICATION_RESULT)
    );
  }

  private static StackTraceTableDataProvider provider(final AuthorizerMapper authorizerMapper)
  {
    return new StackTraceTableDataProvider(
        new DruidNode("coordinator", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.COORDINATOR, NodeRole.OVERLORD),
        authorizerMapper
    );
  }

  private static AuthorizerMapper allowAllAuthorizerMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) -> Access.OK;
      }
    };
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }

  private static long countStackFrames(final String stackTrace)
  {
    return stackTrace.lines().filter(line -> line.startsWith("\tat ")).count();
  }
}
