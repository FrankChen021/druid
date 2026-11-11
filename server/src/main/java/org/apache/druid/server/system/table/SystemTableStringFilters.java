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

import com.google.common.collect.Sets;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads the string values that pushdown filters of a system table require columns to equal, so a
 * {@link SystemTableDataProvider} can read only the rows with those values.
 */
public final class SystemTableStringFilters
{
  private SystemTableStringFilters()
  {
  }

  /**
   * Returns, for each column, the string values that the filters allow it to have: the column must equal one of them.
   * A column constrained by several filters gets the values that all of them allow. Filters that don't require a column
   * to equal one of a set of strings are ignored, since the query applies its whole filter to the provider's rows.
   */
  public static Map<String, Set<String>> getStringValues(final List<Filter> filters)
  {
    final Map<String, Set<String>> result = new HashMap<>();
    for (final Filter filter : filters) {
      final ColumnValues columnValues = getColumnValues(filter);
      if (columnValues != null) {
        result.merge(
            columnValues.column(),
            columnValues.values(),
            (current, values) -> Sets.intersection(current, values).immutableCopy()
        );
      }
    }
    return result;
  }

  /**
   * Returns whether the value is allowed by the values {@link #getStringValues} found for its column, or by no filter.
   */
  public static boolean matches(@Nullable final Set<String> allowedValues, final String value)
  {
    return allowedValues == null || allowedValues.contains(value);
  }

  @Nullable
  private static ColumnValues getColumnValues(final Filter filter)
  {
    if (filter instanceof SelectorFilter selector && selector.getValue() != null) {
      return new ColumnValues(selector.getDimension(), Set.of(selector.getValue()));
    } else if (filter instanceof EqualityFilter equality
               // Other match types compare values as that type, such as "01" equal to "1" as LONG.
               && ColumnType.STRING.equals(equality.getMatchValueType())
               && equality.getMatchValue() instanceof String value) {
      return new ColumnValues(equality.getColumn(), Set.of(value));
    } else if (filter instanceof InDimFilter in && in.getExtractionFn() == null && !in.getValues().contains(null)) {
      return new ColumnValues(in.getDimension(), Set.copyOf(in.getValues()));
    } else if (filter instanceof TypedInFilter in
               && ColumnType.STRING.equals(in.getMatchValueType())
               && in.getSortedValues().stream().allMatch(String.class::isInstance)) {
      final Set<String> values = new HashSet<>();
      in.getSortedValues().forEach(value -> values.add((String) value));
      return new ColumnValues(in.getColumn(), values);
    } else if (filter instanceof OrFilter or) {
      return getColumnValues(or);
    }
    return null;
  }

  /**
   * Returns the values of an OR whose every child requires the same column to equal one of a set of strings.
   */
  @Nullable
  private static ColumnValues getColumnValues(final OrFilter or)
  {
    String column = null;
    final Set<String> values = new HashSet<>();
    for (final Filter child : or.getFilters()) {
      final ColumnValues childValues = getColumnValues(child);
      if (childValues == null || (column != null && !column.equals(childValues.column()))) {
        return null;
      }
      column = childValues.column();
      values.addAll(childValues.values());
    }
    return column == null ? null : new ColumnValues(column, values);
  }

  private record ColumnValues(String column, Set<String> values)
  {
  }
}
