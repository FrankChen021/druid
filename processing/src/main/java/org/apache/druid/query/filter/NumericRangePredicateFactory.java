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

package org.apache.druid.query.filter;

import javax.annotation.Nullable;

/**
 * Optional extension of {@link DruidPredicateFactory} for factories whose numeric predicates accept exactly the
 * non-null values within a single contiguous range. Vectorized numeric matchers use the range to evaluate the filter
 * with SIMD instructions (see {@link org.apache.druid.math.expr.ExpressionProcessing#useVectorApi()}) instead of
 * calling the typed predicate once per row. Null rows are still handled by the typed predicate's
 * {@code applyNull()}.
 */
public interface NumericRangePredicateFactory extends DruidPredicateFactory
{
  /**
   * Returns a range accepting exactly the non-null values accepted by {@link #makeLongPredicate()}, or null if the
   * long predicate cannot be described as a range.
   */
  @Nullable
  LongRange getLongRange();

  /**
   * Returns a range accepting exactly the non-null values accepted by {@link #makeDoublePredicate()}, where values
   * are ordered by {@link Double#compare}, or null if the double predicate cannot be described as a range. The
   * predicate from {@link #makeFloatPredicate()} must be equivalent to this range applied to the input widened to
   * double.
   */
  @Nullable
  DoubleRange getDoubleRange();

  /**
   * Range of long values. If {@code hasLower} is false, {@code lower} is ignored; likewise for the upper bound.
   */
  record LongRange(boolean hasLower, boolean lowerOpen, long lower, boolean hasUpper, boolean upperOpen, long upper)
  {
    public RangeFilter.RangeType rangeType()
    {
      return RangeFilter.RangeType.of(hasLower, lowerOpen, hasUpper, upperOpen);
    }

    /**
     * Whether this range cannot match anything because an open bound sits at the end of the long domain.
     */
    public boolean isEmpty()
    {
      return (hasLower && lowerOpen && lower == Long.MAX_VALUE) || (hasUpper && upperOpen && upper == Long.MIN_VALUE);
    }

    /**
     * Smallest matching value. Only meaningful if {@link #isEmpty()} is false.
     */
    public long lowerInclusive()
    {
      if (!hasLower) {
        return Long.MIN_VALUE;
      }
      return lowerOpen ? lower + 1 : lower;
    }

    /**
     * Largest matching value. Only meaningful if {@link #isEmpty()} is false.
     */
    public long upperInclusive()
    {
      if (!hasUpper) {
        return Long.MAX_VALUE;
      }
      return upperOpen ? upper - 1 : upper;
    }
  }

  /**
   * Range of double values, ordered by {@link Double#compare}. If {@code hasLower} is false, {@code lower} is
   * ignored; likewise for the upper bound.
   */
  record DoubleRange(
      boolean hasLower,
      boolean lowerOpen,
      double lower,
      boolean hasUpper,
      boolean upperOpen,
      double upper
  )
  {
    public RangeFilter.RangeType rangeType()
    {
      return RangeFilter.RangeType.of(hasLower, lowerOpen, hasUpper, upperOpen);
    }
  }
}
