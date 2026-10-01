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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class SystemTablePushdownFilterTest
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", "node"),
      new SystemTablePushdownFilter("service_name", null)
  );

  /** Supported equality and IN conjuncts are extracted and rewritten to provider-side column names. */
  @Test
  public void testExtractSupportedConjuncts()
  {
    final List<DimFilter> extracted = SystemTablePushdownFilter.extract(
        new AndDimFilter(
            List.of(
                new SelectorDimFilter("server", "node-a", null),
                new EqualityFilter("service_name", ColumnType.STRING, "broker", null),
                new InDimFilter("server", Set.of("node-b", "node-c"), null),
                new TypedInFilter("service_name", ColumnType.STRING, List.of("historical", "router"), null, null),
                new LikeDimFilter("server", "node-%", null, null)
            )
        ),
        PUSHDOWN_FILTERS
    );

    Assertions.assertEquals(4, extracted.size());
    Assertions.assertEquals("node", SystemTablePushdownFilter.getStringValuesColumn(extracted.get(0)));
    Assertions.assertEquals(Set.of("node-a"), SystemTablePushdownFilter.getStringValues(extracted.get(0)));
    Assertions.assertEquals("service_name", SystemTablePushdownFilter.getStringValuesColumn(extracted.get(1)));
    Assertions.assertEquals(Set.of("broker"), SystemTablePushdownFilter.getStringValues(extracted.get(1)));
    Assertions.assertEquals(Set.of("node-b", "node-c"), SystemTablePushdownFilter.getStringValues(extracted.get(2)));
    Assertions.assertEquals(
        Set.of("historical", "router"),
        SystemTablePushdownFilter.getStringValues(extracted.get(3))
    );
  }

  /** Query-level extraction uses the query filter and leaves unsupported predicates for residual evaluation. */
  @Test
  public void testExtractFromQuery()
  {
    final List<DimFilter> extracted = SystemTablePushdownFilter.extract(
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .filters(new SelectorDimFilter("server", "node-a", null))
              .build(),
        PUSHDOWN_FILTERS
    );

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals("node", SystemTablePushdownFilter.getStringValuesColumn(extracted.get(0)));
  }

  /** Empty rules, absent filters, and predicates on undeclared columns produce no pushdown filters. */
  @Test
  public void testExtractWithoutEligibleFilter()
  {
    final DimFilter selector = new SelectorDimFilter("server", "node-a", null);

    Assertions.assertTrue(SystemTablePushdownFilter.extract(selector, List.of()).isEmpty());
    Assertions.assertTrue(SystemTablePushdownFilter.extract((DimFilter) null, PUSHDOWN_FILTERS).isEmpty());
    Assertions.assertTrue(
        SystemTablePushdownFilter.extract(
            new SelectorDimFilter("property_name", "druid.test", null),
            PUSHDOWN_FILTERS
        ).isEmpty()
    );
  }

  /** Filters with nulls, extraction functions, non-string types, or empty value sets remain residual-only. */
  @Test
  public void testRejectsUnsafeStringValueFilters()
  {
    final List<DimFilter> filters = List.of(
        new SelectorDimFilter("server", null, null),
        new SelectorDimFilter("server", "node-a", IdentityExtractionFn.getInstance()),
        new EqualityFilter("server", ColumnType.LONG, 1L, null),
        new InDimFilter("server", List.of(), null),
        new InDimFilter("server", Set.of("node-a"), IdentityExtractionFn.getInstance()),
        new TypedInFilter("server", ColumnType.LONG, List.of(1L), null, null),
        new TypedInFilter("server", ColumnType.STRING, List.of(), null, null)
    );

    for (final DimFilter filter : filters) {
      Assertions.assertTrue(SystemTablePushdownFilter.extract(filter, PUSHDOWN_FILTERS).isEmpty(), filter.toString());
    }
  }

  /** OR is pushed down only when every branch is a supported finite-value filter on the same column. */
  @Test
  public void testExtractOrFilter()
  {
    final OrDimFilter supported = new OrDimFilter(
        List.of(
            new SelectorDimFilter("server", "node-a", null),
            new EqualityFilter("server", ColumnType.STRING, "node-b", null)
        )
    );
    final OrDimFilter differentColumns = new OrDimFilter(
        List.of(
            new SelectorDimFilter("server", "node-a", null),
            new SelectorDimFilter("service_name", "broker", null)
        )
    );
    final OrDimFilter unsupportedBranch = new OrDimFilter(
        List.of(
            new SelectorDimFilter("server", "node-a", null),
            new LikeDimFilter("server", "node-%", null, null)
        )
    );

    final List<DimFilter> extracted = SystemTablePushdownFilter.extract(supported, PUSHDOWN_FILTERS);
    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals(Set.of("node-a", "node-b"), SystemTablePushdownFilter.getStringValues(extracted.get(0)));
    Assertions.assertTrue(SystemTablePushdownFilter.extract(differentColumns, PUSHDOWN_FILTERS).isEmpty());
    Assertions.assertTrue(SystemTablePushdownFilter.extract(unsupportedBranch, PUSHDOWN_FILTERS).isEmpty());
  }

  /** Finite-value helpers reject arbitrary filters rather than treating them as provider predicates. */
  @Test
  public void testStringValuesHelpersRejectUnsupportedFilters()
  {
    final DimFilter unsupported = new LikeDimFilter("server", "node-%", null, null);

    Assertions.assertFalse(SystemTablePushdownFilter.isStringValuesFilter(unsupported));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SystemTablePushdownFilter.getStringValuesColumn(unsupported)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SystemTablePushdownFilter.getStringValues(unsupported)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SystemTablePushdownFilter.getStringValuesColumn(null)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SystemTablePushdownFilter.getStringValues(null)
    );
  }
}
