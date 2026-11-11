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
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads the values that a filter on a system table allows for one of its string columns, such as {@code server}.
 *
 * <p>The Broker uses this to choose which services to query, and the services use it to skip collecting rows that the
 * filter rejects. Both are best-effort: the query still applies its whole filter to the rows, so a filter that this
 * class can't read only means that more services are queried or more rows are collected.</p>
 */
public final class SystemTableFilters
{
  private SystemTableFilters()
  {
  }

  /**
   * Returns the values one of which the filter requires the column to equal, or null if the filter doesn't restrict
   * the column in a way this method understands. An empty set means that no value matches.
   *
   * <p>The filter is split into its {@code AND} conjuncts. A conjunct restricts the column if it is {@code =},
   * {@code IN}, or an {@code OR} of them on that same column, comparing values as strings. The sets of the conjuncts
   * that restrict the column are intersected. Other conjuncts, such as {@code LIKE}, {@code NOT}, or an {@code OR}
   * across columns, are ignored.</p>
   */
  @Nullable
  public static Set<String> getStringValues(@Nullable final Filter filter, final String column)
  {
    return filter == null ? null : getStringValues(List.of(filter), column);
  }

  /**
   * Like {@link #getStringValues(Filter, String)}, for the conjuncts of a filter.
   */
  @Nullable
  public static Set<String> getStringValues(final Iterable<Filter> conjuncts, final String column)
  {
    Set<String> values = null;
    for (final Filter conjunct : conjuncts) {
      final Set<String> conjunctValues = conjunct instanceof AndFilter andFilter
                                         ? getStringValues(andFilter.getFilters(), column)
                                         : getConjunctStringValues(conjunct, column);
      if (conjunctValues != null) {
        if (values == null) {
          values = new HashSet<>(conjunctValues);
        } else {
          values.retainAll(conjunctValues);
        }
      }
    }
    return values;
  }

  /**
   * Whether the filter conjuncts allow the column to have the given value, according to
   * {@link #getStringValues(Iterable, String)}.
   */
  public static boolean allowsValue(final Iterable<Filter> conjuncts, final String column, final String value)
  {
    final Set<String> values = getStringValues(conjuncts, column);
    return values == null || values.contains(value);
  }

  /**
   * Returns the values one of which a filter that isn't an {@code AND} requires the column to equal, or null if it
   * isn't such a filter.
   */
  @Nullable
  private static Set<String> getConjunctStringValues(final Filter filter, final String column)
  {
    if (filter instanceof SelectorFilter selector) {
      return column.equals(selector.getDimension()) && selector.getValue() != null
             ? Set.of(selector.getValue())
             : null;
    } else if (filter instanceof EqualityFilter equality) {
      // Other match types compare values as that type, such as "01" equal to "1" as LONG.
      return column.equals(equality.getColumn())
             && ColumnType.STRING.equals(equality.getMatchValueType())
             && equality.getMatchValue() instanceof String value
             ? Set.of(value)
             : null;
    } else if (filter instanceof InDimFilter in) {
      return column.equals(in.getDimension())
             && in.getExtractionFn() == null
             && !in.getValues().isEmpty()
             && !in.getValues().contains(null)
             ? Set.copyOf(in.getValues())
             : null;
    } else if (filter instanceof TypedInFilter in) {
      if (!column.equals(in.getColumn())
          || !ColumnType.STRING.equals(in.getMatchValueType())
          || in.getSortedValues().isEmpty()) {
        return null;
      }
      final Set<String> values = new HashSet<>();
      for (final Object value : in.getSortedValues()) {
        if (!(value instanceof String stringValue)) {
          return null;
        }
        values.add(stringValue);
      }
      return values;
    } else if (filter instanceof OrFilter or) {
      final Set<String> values = new HashSet<>();
      for (final Filter child : or.getFilters()) {
        final Set<String> childValues = getConjunctStringValues(child, column);
        if (childValues == null) {
          return null;
        }
        values.addAll(childValues);
      }
      return values.isEmpty() ? null : values;
    }
    return null;
  }
}
