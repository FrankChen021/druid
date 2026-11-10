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

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class SystemTableFiltersTest
{
  private static final String SERVER = "server";

  @Test
  public void testNoFilterDoesNotRestrict()
  {
    Assertions.assertNull(SystemTableFilters.getStringValues((Filter) null, SERVER));
    Assertions.assertNull(SystemTableFilters.getStringValues(List.of(), SERVER));
    Assertions.assertTrue(SystemTableFilters.allowsValue(List.of(), SERVER, "a:1"));
  }

  @Test
  public void testEquality()
  {
    Assertions.assertEquals(Set.of("a:1"), values(equality(SERVER, "a:1")));
    Assertions.assertEquals(
        Set.of("a:1"),
        SystemTableFilters.getStringValues(new SelectorFilter(SERVER, "a:1"), SERVER)
    );
  }

  @Test
  public void testIn()
  {
    Assertions.assertEquals(Set.of("a:1", "b:2"), values(new InDimFilter(SERVER, Set.of("a:1", "b:2"))));
    Assertions.assertEquals(
        Set.of("a:1", "b:2"),
        SystemTableFilters.getStringValues(
            new TypedInFilter(SERVER, ColumnType.STRING, List.of("a:1", "b:2"), null, null),
            SERVER
        )
    );
  }

  @Test
  public void testOrOfEqualitiesAndInsOnTheSameColumn()
  {
    final Filter filter = new OrFilter(
        List.of(
            new SelectorFilter(SERVER, "a:1"),
            Filters.toFilter(new InDimFilter(SERVER, Set.of("b:2", "c:3")))
        )
    );
    Assertions.assertEquals(Set.of("a:1", "b:2", "c:3"), SystemTableFilters.getStringValues(filter, SERVER));
  }

  @Test
  public void testConjunctsOnTheSameColumnIntersect()
  {
    final Filter filter = new AndFilter(
        List.of(
            Filters.toFilter(new InDimFilter(SERVER, Set.of("a:1", "b:2", "c:3"))),
            Filters.toFilter(new InDimFilter(SERVER, Set.of("b:2", "c:3", "d:4"))),
            Filters.toFilter(equality("service_name", "druid/broker"))
        )
    );
    Assertions.assertEquals(Set.of("b:2", "c:3"), SystemTableFilters.getStringValues(filter, SERVER));
    Assertions.assertEquals(Set.of("druid/broker"), SystemTableFilters.getStringValues(filter, "service_name"));
    Assertions.assertTrue(SystemTableFilters.allowsValue(List.of(filter), SERVER, "b:2"));
    Assertions.assertFalse(SystemTableFilters.allowsValue(List.of(filter), SERVER, "a:1"));
  }

  @Test
  public void testDisjointConjunctsAllowNoValue()
  {
    final List<Filter> conjuncts = List.of(
        new SelectorFilter(SERVER, "a:1"),
        new SelectorFilter(SERVER, "b:2")
    );
    Assertions.assertEquals(Set.of(), SystemTableFilters.getStringValues(conjuncts, SERVER));
    Assertions.assertFalse(SystemTableFilters.allowsValue(conjuncts, SERVER, "a:1"));
  }

  @Test
  public void testNestedAndIsSplit()
  {
    final Filter filter = new AndFilter(
        List.of(
            new AndFilter(List.of(new SelectorFilter(SERVER, "a:1"), new SelectorFilter("service_name", "x"))),
            Filters.toFilter(new InDimFilter(SERVER, Set.of("a:1", "b:2")))
        )
    );
    Assertions.assertEquals(Set.of("a:1"), SystemTableFilters.getStringValues(filter, SERVER));
  }

  @Test
  public void testFiltersThatDoNotRestrict()
  {
    Assertions.assertNull(values(new LikeDimFilter(SERVER, "a%", null, null)));
    Assertions.assertNull(values(new NotDimFilter(equality(SERVER, "a:1"))));
    Assertions.assertNull(values(equality("service_name", "druid/broker")));
    // An OR across columns.
    Assertions.assertNull(
        SystemTableFilters.getStringValues(
            new OrFilter(List.of(new SelectorFilter(SERVER, "a:1"), new SelectorFilter("service_name", "x"))),
            SERVER
        )
    );
    // An OR containing an AND.
    Assertions.assertNull(
        SystemTableFilters.getStringValues(
            new OrFilter(
                List.of(
                    new SelectorFilter(SERVER, "a:1"),
                    new AndFilter(List.of(new SelectorFilter(SERVER, "b:2"), new SelectorFilter("service_name", "x")))
                )
            ),
            SERVER
        )
    );
    // A null value, as in IS NULL.
    Assertions.assertNull(SystemTableFilters.getStringValues(new SelectorFilter(SERVER, null), SERVER));
    // A non-string match type compares values as that type.
    Assertions.assertNull(
        SystemTableFilters.getStringValues(new EqualityFilter(SERVER, ColumnType.LONG, 1L, null), SERVER)
    );
    Assertions.assertNull(
        SystemTableFilters.getStringValues(
            new TypedInFilter(SERVER, ColumnType.LONG, List.of(1L, 2L), null, null),
            SERVER
        )
    );
  }

  @Test
  public void testNonRestrictingConjunctsAreIgnored()
  {
    final Filter filter = new AndFilter(
        List.of(
            Filters.toFilter(new LikeDimFilter(SERVER, "a%", null, null)),
            new SelectorFilter(SERVER, "a:1")
        )
    );
    Assertions.assertEquals(Set.of("a:1"), SystemTableFilters.getStringValues(filter, SERVER));
  }

  private static Set<String> values(final DimFilter filter)
  {
    return SystemTableFilters.getStringValues(Filters.toFilter(filter), SERVER);
  }

  private static EqualityFilter equality(final String column, final String value)
  {
    return new EqualityFilter(column, ColumnType.STRING, value, null);
  }
}
