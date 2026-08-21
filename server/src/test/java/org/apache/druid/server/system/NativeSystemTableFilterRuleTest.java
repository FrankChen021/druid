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

import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NativeSystemTableFilterRuleTest
{
  @Test
  public void testExactStringRuleSupportsSelectorAndEquality()
  {
    final NativeSystemTableFilterRule rule = NativeSystemTableFilterRule.exactString("server");

    Assertions.assertTrue(rule.matches(new SelectorDimFilter("server", "localhost:8080", null)));
    Assertions.assertFalse(rule.matches(new SelectorDimFilter("other", "localhost:8080", null)));
    Assertions.assertTrue(rule.matches(new EqualityFilter("server", ColumnType.STRING, "localhost:8080", null)));
    Assertions.assertFalse(rule.matches(new EqualityFilter("server", ColumnType.LONG, 8080L, null)));
    Assertions.assertFalse(rule.matches(new InDimFilter("server", List.of("localhost:8080"), null)));
  }

  @Test
  public void testStringValuesRuleSupportsAllNativeStringShapes()
  {
    final NativeSystemTableFilterRule rule = NativeSystemTableFilterRule.stringValues("task_id");

    Assertions.assertTrue(rule.matches(new InDimFilter("task_id", List.of("task-a", "task-b"), null)));
    Assertions.assertFalse(rule.matches(new InDimFilter("task_id", List.of(), null)));
    Assertions.assertTrue(
        rule.matches(new TypedInFilter("task_id", ColumnType.STRING, List.of("task-a"), null, null))
    );
    Assertions.assertFalse(
        rule.matches(new TypedInFilter("task_id", ColumnType.LONG, List.of(1L), null, null))
    );
    Assertions.assertTrue(
        rule.matches(
            new OrDimFilter(
                new SelectorDimFilter("task_id", "task-a", null),
                new EqualityFilter("task_id", ColumnType.STRING, "task-b", null)
            )
        )
    );
    Assertions.assertFalse(
        rule.matches(new OrDimFilter(new SelectorDimFilter("other", "task-a", null)))
    );
    Assertions.assertFalse(rule.matches(new RangeFilter("task_id", ColumnType.STRING, "a", "z", false, false, null)));
  }

  @Test
  public void testLexicographicRangeRuleSupportsBoundAndRangeFilters()
  {
    final NativeSystemTableFilterRule rule = NativeSystemTableFilterRule.lexicographicStringRange("created_time");

    Assertions.assertTrue(
        rule.matches(
            new BoundDimFilter(
                "created_time",
                "2026-01-01",
                "2026-02-01",
                false,
                true,
                null,
                null,
                StringComparators.LEXICOGRAPHIC
            )
        )
    );
    Assertions.assertFalse(
        rule.matches(
            new BoundDimFilter(
                "created_time",
                "2026-01-01",
                "2026-02-01",
                false,
                true,
                null,
                null,
                StringComparators.NUMERIC
            )
        )
    );
    Assertions.assertTrue(
        rule.matches(new RangeFilter("created_time", ColumnType.STRING, null, "2026-02-01", false, true, null))
    );
    Assertions.assertFalse(
        rule.matches(new RangeFilter("created_time", ColumnType.LONG, 1L, 2L, false, false, null))
    );
    Assertions.assertFalse(rule.matches(new SelectorDimFilter("created_time", "2026-01-01", null)));
  }
}
