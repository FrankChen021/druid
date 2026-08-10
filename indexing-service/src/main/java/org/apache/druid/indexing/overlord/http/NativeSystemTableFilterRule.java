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

package org.apache.druid.indexing.overlord.http;

import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;

import java.util.Objects;

/** Declares a native filter shape that a system-table data supplier can translate to its storage API. */
public interface NativeSystemTableFilterRule
{
  boolean matches(DimFilter filter);

  static NativeSystemTableFilterRule exactString(final String column)
  {
    return filter -> {
      if (filter instanceof SelectorDimFilter) {
        final SelectorDimFilter selector = (SelectorDimFilter) filter;
        return column.equals(selector.getDimension())
               && selector.getValue() != null
               && selector.getExtractionFn() == null;
      } else if (filter instanceof EqualityFilter) {
        final EqualityFilter equality = (EqualityFilter) filter;
        return column.equals(equality.getColumn())
               && ColumnType.STRING.equals(equality.getMatchValueType())
               && equality.getMatchValue() instanceof String;
      }
      return false;
    };
  }

  static NativeSystemTableFilterRule stringValues(final String column)
  {
    return filter -> matchesStringValues(filter, column);
  }

  static NativeSystemTableFilterRule lexicographicStringRange(final String column)
  {
    return filter -> {
      if (filter instanceof BoundDimFilter) {
        final BoundDimFilter bound = (BoundDimFilter) filter;
        return column.equals(bound.getDimension())
               && bound.getExtractionFn() == null
               && StringComparators.LEXICOGRAPHIC.equals(bound.getOrdering());
      } else if (filter instanceof RangeFilter) {
        final RangeFilter range = (RangeFilter) filter;
        return column.equals(range.getColumn())
               && ColumnType.STRING.equals(range.getMatchValueType())
               && (range.getLower() == null || range.getLower() instanceof String)
               && (range.getUpper() == null || range.getUpper() instanceof String);
      }
      return false;
    };
  }

  private static boolean matchesStringValues(final DimFilter filter, final String column)
  {
    if (filter instanceof SelectorDimFilter) {
      final SelectorDimFilter selector = (SelectorDimFilter) filter;
      return column.equals(selector.getDimension())
             && selector.getValue() != null
             && selector.getExtractionFn() == null;
    } else if (filter instanceof EqualityFilter) {
      final EqualityFilter equality = (EqualityFilter) filter;
      return column.equals(equality.getColumn())
             && ColumnType.STRING.equals(equality.getMatchValueType())
             && equality.getMatchValue() instanceof String;
    } else if (filter instanceof InDimFilter) {
      final InDimFilter in = (InDimFilter) filter;
      return column.equals(in.getDimension())
             && in.getExtractionFn() == null
             && !in.getValues().isEmpty()
             && in.getValues().stream().allMatch(Objects::nonNull);
    } else if (filter instanceof TypedInFilter) {
      final TypedInFilter in = (TypedInFilter) filter;
      return column.equals(in.getColumn())
             && ColumnType.STRING.equals(in.getMatchValueType())
             && !in.getSortedValues().isEmpty()
             && in.getSortedValues().stream().allMatch(String.class::isInstance);
    } else if (filter instanceof OrDimFilter) {
      final OrDimFilter or = (OrDimFilter) filter;
      return !or.getFields().isEmpty()
             && or.getFields().stream().allMatch(field -> matchesStringValues(field, column));
    }
    return false;
  }
}
