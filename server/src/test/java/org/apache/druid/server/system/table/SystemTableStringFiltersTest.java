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

import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SystemTableStringFiltersTest
{
  @Test
  public void testReadsStringValuesOfEqualityAndInFilters()
  {
    final List<Filter> filters = List.of(
        new SelectorFilter("server", "a"),
        new EqualityFilter("datasource", ColumnType.STRING, "foo", null),
        new InDimFilter("version", Set.of("v1", "v2")),
        new TypedInFilter("segment_id", ColumnType.STRING, List.of("s1", "s2"), null, null),
        new OrFilter(List.of(new SelectorFilter("start", "x"), new SelectorFilter("start", "y")))
    );

    Assertions.assertEquals(
        Map.of(
            "server", Set.of("a"),
            "datasource", Set.of("foo"),
            "version", Set.of("v1", "v2"),
            "segment_id", Set.of("s1", "s2"),
            "start", Set.of("x", "y")
        ),
        SystemTableStringFilters.getStringValues(filters)
    );
  }

  @Test
  public void testIntersectsValuesOfOneColumn()
  {
    final List<Filter> filters = List.of(
        new InDimFilter("datasource", Set.of("foo", "bar")),
        new SelectorFilter("datasource", "foo")
    );

    Assertions.assertEquals(
        Map.of("datasource", Set.of("foo")),
        SystemTableStringFilters.getStringValues(filters)
    );
  }

  /**
   * Filters that don't require a column to equal one of a set of strings are ignored.
   */
  @Test
  public void testIgnoresOtherFilters()
  {
    final List<Filter> filters = List.of(
        new EqualityFilter("size", ColumnType.LONG, 1L, null),
        new RangeFilter("datasource", ColumnType.STRING, "a", "b", false, false, null),
        new OrFilter(List.of(new SelectorFilter("start", "x"), new SelectorFilter("end", "y"))),
        new SelectorFilter("version", null)
    );

    Assertions.assertEquals(Map.of(), SystemTableStringFilters.getStringValues(filters));
  }

  @Test
  public void testMatches()
  {
    Assertions.assertTrue(SystemTableStringFilters.matches(null, "foo"));
    Assertions.assertTrue(SystemTableStringFilters.matches(Set.of("foo"), "foo"));
    Assertions.assertFalse(SystemTableStringFilters.matches(Set.of("bar"), "foo"));
  }
}
