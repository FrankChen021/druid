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

import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.system.table.ServerPropertiesTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

public class ServerPropertiesTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  @Test
  public void testReturnsVisiblePropertiesAndNodeMetadata()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.test.visible", "visible");
    properties.setProperty("druid.test.password", "hidden");

    final ServerPropertiesTableDataProvider supplier = supplier(properties, allowAllAuthorizerMapper());
    final List<Object[]> rows = toRows(supplier.getRows(List.of(), AUTHENTICATION_RESULT));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(
        new Object[]{
            "localhost:8080",
            "overlord",
            "[overlord]",
            "druid.test.visible",
            "visible",
            null
        },
        rows.get(0)
    );
  }

  /**
   * Server and service-name value filters prune a node that cannot match before its properties are read, while
   * predicates the provider cannot evaluate are left for residual evaluation and never prune.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("filterCases")
  public void testAppliesPushdownFilters(final String description, final DimFilter filter, final boolean expectRows)
  {
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties(), allowAllAuthorizerMapper());

    Assertions.assertEquals(
        expectRows,
        !toRows(supplier.getRows(List.of(filter), AUTHENTICATION_RESULT)).isEmpty()
    );
  }

  private static Stream<Arguments> filterCases()
  {
    return Stream.of(
        Arguments.of("matching server", new SelectorDimFilter("server", "localhost:8080", null), true),
        Arguments.of("other server", new SelectorDimFilter("server", "other:8080", null), false),
        Arguments.of("other service", new SelectorDimFilter("service_name", "broker", null), false),
        Arguments.of(
            "server IN containing this node",
            new InDimFilter("server", Set.of("other:8080", "localhost:8080"), null),
            true
        ),
        Arguments.of(
            "server OR without this node",
            new OrDimFilter(
                List.of(
                    new SelectorDimFilter("server", "other:8080", null),
                    new SelectorDimFilter("server", "another:8080", null)
                )
            ),
            false
        ),
        Arguments.of(
            "service OR containing this node",
            new OrDimFilter(
                List.of(
                    new SelectorDimFilter("service_name", "broker", null),
                    new SelectorDimFilter("service_name", "overlord", null)
                )
            ),
            true
        ),
        Arguments.of("unsupported LIKE is residual", new LikeDimFilter("server", "other%", null, null), true)
    );
  }

  @Test
  public void testReturnsPlaceholderWhenNoPropertiesExist()
  {
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties(), allowAllAuthorizerMapper());
    final List<Object[]> rows = toRows(supplier.getRows(List.of(), AUTHENTICATION_RESULT));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertNull(rows.get(0)[3]);
    Assertions.assertNull(rows.get(0)[4]);
    Assertions.assertNull(rows.get(0)[5]);
  }

  @Test
  public void testRejectsUnauthorizedRequest()
  {
    final Authorizer denyAll = (authenticationResult, resource, action) -> Access.DENIED;
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return denyAll;
      }
    };
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties(), authorizerMapper);

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> supplier.getRows(List.of(), AUTHENTICATION_RESULT)
    );
  }

  private static ServerPropertiesTableDataProvider supplier(
      final Properties properties,
      final AuthorizerMapper authorizerMapper
  )
  {
    return new ServerPropertiesTableDataProvider(
        new DruidNode("overlord", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.OVERLORD),
        authorizerMapper,
        properties,
        new DruidServerConfig(null, null)
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
    final List<Object[]> result = new java.util.ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
