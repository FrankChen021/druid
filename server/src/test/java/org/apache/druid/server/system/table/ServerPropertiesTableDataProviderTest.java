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

import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ServerPropertiesTableDataProviderTest
{
  @Test
  public void testReturnsVisiblePropertiesAndNodeMetadata()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.test.visible", "visible");
    properties.setProperty("druid.test.password", "hidden");

    final ServerPropertiesTableDataProvider supplier = supplier(properties);
    final List<Object[]> rows = toRows(supplier.getRows(List.of()));

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
   * Configured {@code druid.server.hiddenProperties} are matched as case-insensitive substrings of property names.
   */
  @Test
  public void testMasksConfiguredHiddenProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.test.visible", "visible");
    properties.setProperty("druid.test.MySecretValue", "hidden");
    properties.setProperty("druid.mysecret.endpoint", "hidden");

    final ServerPropertiesTableDataProvider supplier = new ServerPropertiesTableDataProvider(
        new DruidNode("overlord", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.OVERLORD),
        properties,
        new DruidServerConfig(null, null)
        {
          @Override
          public Set<String> getHiddenProperties()
          {
            return Set.of("mySecret");
          }
        }
    );
    final List<Object[]> rows = toRows(supplier.getRows(List.of()));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("druid.test.visible", rows.getFirst()[3]);
  }

  @Test
  public void testAppliesServerAndServiceNameFilters()
  {
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties());
    final Filter wrongServer = new SelectorDimFilter("server", "other:8080", null).toFilter();
    final Filter wrongService = new EqualityFilter("service_name", ColumnType.STRING, "broker", null);

    Assertions.assertTrue(toRows(supplier.getRows(List.of(wrongServer))).isEmpty());
    Assertions.assertTrue(toRows(supplier.getRows(List.of(wrongService))).isEmpty());
    Assertions.assertFalse(
        toRows(
            supplier.getRows(List.of(new SelectorDimFilter("server", "localhost:8080", null).toFilter()))
        ).isEmpty()
    );
    Assertions.assertFalse(
        toRows(
            supplier.getRows(List.of(new EqualityFilter("server", ColumnType.STRING, "localhost:8080", null)))
        ).isEmpty()
    );
  }

  /**
   * The provider skips its rows when an {@code IN} or an {@code OR} of equalities on {@code server} or
   * {@code service_name} excludes it, and returns them when it includes it.
   */
  @Test
  public void testAppliesInAndOrFilters()
  {
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties());

    Assertions.assertTrue(
        toRows(supplier.getRows(List.of(new InDimFilter("server", Set.of("a:1", "b:2")).toFilter()))).isEmpty()
    );
    Assertions.assertTrue(
        toRows(supplier.getRows(List.of(new InDimFilter("service_name", Set.of("broker")).toFilter()))).isEmpty()
    );
    Assertions.assertTrue(
        toRows(supplier.getRows(List.of(or("server", "a:1", "b:2")))).isEmpty()
    );
    Assertions.assertFalse(
        toRows(supplier.getRows(List.of(new InDimFilter("server", Set.of("a:1", "localhost:8080")).toFilter())))
            .isEmpty()
    );
    Assertions.assertFalse(
        toRows(supplier.getRows(List.of(new InDimFilter("service_name", Set.of("broker", "overlord")).toFilter())))
            .isEmpty()
    );
    Assertions.assertFalse(
        toRows(supplier.getRows(List.of(or("server", "a:1", "localhost:8080")))).isEmpty()
    );
  }

  /**
   * A provider with TLS enabled reports, and is matched by, its TLS {@code host:port}, which differs from its plaintext
   * one. This is the value that the Broker matches against discovery.
   */
  @Test
  public void testTlsNodeIsMatchedByTlsHostAndPort()
  {
    final DruidNode tlsNode = new DruidNode("router", "localhost", false, 8888, 9088, true, true);
    final ServerPropertiesTableDataProvider supplier = new ServerPropertiesTableDataProvider(
        tlsNode,
        Set.of(NodeRole.ROUTER),
        new Properties(),
        new DruidServerConfig(null, null)
    );

    final List<Object[]> rows = toRows(
        supplier.getRows(List.of(new EqualityFilter("server", ColumnType.STRING, "localhost:9088", null)))
    );
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("localhost:9088", rows.getFirst()[0]);
    Assertions.assertEquals(NodeAttribute.HOST_AND_PORT.getValue(tlsNode), rows.getFirst()[0]);
    Assertions.assertTrue(
        toRows(
            supplier.getRows(List.of(new EqualityFilter("server", ColumnType.STRING, "localhost:8888", null)))
        ).isEmpty()
    );
  }

  /**
   * An equality filter compares values as its match type. A LONG equality on "01" matches the service name "1", so the
   * provider must not drop the row by comparing the strings.
   */
  @Test
  public void testDoesNotCompareNonStringEqualityAsStrings()
  {
    final ServerPropertiesTableDataProvider supplier = supplier("1", new Properties());
    final Filter numericEquality = new EqualityFilter("service_name", ColumnType.LONG, "01", null);

    Assertions.assertFalse(toRows(supplier.getRows(List.of(numericEquality))).isEmpty());
  }

  @Test
  public void testReturnsPlaceholderWhenNoPropertiesExist()
  {
    final ServerPropertiesTableDataProvider supplier = supplier(new Properties());
    final List<Object[]> rows = toRows(supplier.getRows(List.of()));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertNull(rows.get(0)[3]);
    Assertions.assertNull(rows.get(0)[4]);
    Assertions.assertNull(rows.get(0)[5]);
  }

  private static ServerPropertiesTableDataProvider supplier(final Properties properties)
  {
    return supplier("overlord", properties);
  }

  private static ServerPropertiesTableDataProvider supplier(final String serviceName, final Properties properties)
  {
    return new ServerPropertiesTableDataProvider(
        new DruidNode(serviceName, "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.OVERLORD),
        properties,
        new DruidServerConfig(null, null)
    );
  }

  private static Filter or(final String column, final String... values)
  {
    return new OrFilter(
        Arrays.stream(values)
              .map(value -> (Filter) new EqualityFilter(column, ColumnType.STRING, value, null))
              .toList()
    );
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new java.util.ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
