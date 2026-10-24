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

package org.apache.druid.query.filter.vector.simd;

import org.apache.druid.query.filter.NumericRangePredicateFactory;

/**
 * Scalar helpers shared by the SIMD numeric vector value matchers.
 *
 * Floating point range filters compare with {@link Double#compare} ordering, in which {@code -0.0 < 0.0} and NaN
 * is greater than every other value. SIMD lane comparisons use IEEE 754 ordering instead, so the range matchers
 * compare "sort keys": the raw bits of each value, with the magnitude bits flipped for negative values, and all NaNs
 * replaced by the key of the canonical NaN. Signed integer comparison of these keys is exactly {@link Double#compare}
 * (or {@link Float#compare}) ordering.
 */
final class SimdMatcherSupport
{
  /**
   * Sort key of {@link Double#NaN}, which {@link Double#compare} treats as equal to every other NaN.
   */
  static final long DOUBLE_NAN_KEY = doubleKey(Double.NaN);

  /**
   * Sort key of {@link Float#NaN}, which {@link Float#compare} treats as equal to every other NaN.
   */
  static final int FLOAT_NAN_KEY = floatKey(Float.NaN);

  /**
   * Smallest sort key of a float, after NaN canonicalization.
   */
  static final int FLOAT_MIN_KEY = floatKey(Float.NEGATIVE_INFINITY);

  private SimdMatcherSupport()
  {
  }

  /**
   * Appends the row numbers {@code offset + bit} of every set bit of {@code bits} (as produced by
   * {@code VectorMask#toLong()}) to {@code selection}, starting at {@code numRows}. Returns the new number of rows.
   * Iterating set bits is used instead of {@code IntVector#compress}, which is not intrinsified on every platform.
   */
  static int appendMatches(final int[] selection, int numRows, final int offset, long bits)
  {
    while (bits != 0) {
      selection[numRows++] = offset + Long.numberOfTrailingZeros(bits);
      bits &= bits - 1;
    }
    return numRows;
  }

  static long doubleKey(final double value)
  {
    final long bits = Double.doubleToLongBits(value);
    return bits ^ ((bits >> 63) & Long.MAX_VALUE);
  }

  static int floatKey(final float value)
  {
    final int bits = Float.floatToIntBits(value);
    return bits ^ ((bits >> 31) & Integer.MAX_VALUE);
  }

  /**
   * Inverse of {@link #floatKey} for non-NaN keys.
   */
  static float floatFromKey(final int key)
  {
    return Float.intBitsToFloat(key ^ ((key >> 31) & Integer.MAX_VALUE));
  }

  /**
   * Smallest sort key accepted by the lower bound of the range. Greater than {@link #DOUBLE_NAN_KEY} if nothing is
   * accepted.
   */
  static long doubleLowerKeyInclusive(final NumericRangePredicateFactory.DoubleRange range)
  {
    if (!range.hasLower()) {
      return Long.MIN_VALUE;
    }
    // cannot overflow, the largest possible key is DOUBLE_NAN_KEY
    return range.lowerOpen() ? doubleKey(range.lower()) + 1 : doubleKey(range.lower());
  }

  /**
   * Largest sort key accepted by the upper bound of the range.
   */
  static long doubleUpperKeyInclusive(final NumericRangePredicateFactory.DoubleRange range)
  {
    if (!range.hasUpper()) {
      return Long.MAX_VALUE;
    }
    // cannot underflow, the smallest possible key is that of negative infinity
    return range.upperOpen() ? doubleKey(range.upper()) - 1 : doubleKey(range.upper());
  }

  /**
   * Smallest float sort key whose value, widened to double, is accepted by the lower bound of the range. Greater
   * than {@link #FLOAT_NAN_KEY} if nothing is accepted. Widening is monotonic, so this is a binary search over the
   * float keys.
   */
  static int floatLowerKeyInclusive(final NumericRangePredicateFactory.DoubleRange range)
  {
    if (!range.hasLower()) {
      return FLOAT_MIN_KEY;
    }
    final long target = doubleLowerKeyInclusive(range);
    long lo = FLOAT_MIN_KEY;
    long hi = (long) FLOAT_NAN_KEY + 1;
    while (lo < hi) {
      final long mid = (lo + hi) >> 1;
      if (widenedFloatKey((int) mid) >= target) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    return (int) lo;
  }

  /**
   * Largest float sort key whose value, widened to double, is accepted by the upper bound of the range. Smaller than
   * {@link #FLOAT_MIN_KEY} if nothing is accepted.
   */
  static int floatUpperKeyInclusive(final NumericRangePredicateFactory.DoubleRange range)
  {
    if (!range.hasUpper()) {
      return FLOAT_NAN_KEY;
    }
    final long target = doubleUpperKeyInclusive(range);
    long lo = (long) FLOAT_MIN_KEY - 1;
    long hi = FLOAT_NAN_KEY;
    while (lo < hi) {
      final long mid = (lo + hi + 1) >> 1;
      if (widenedFloatKey((int) mid) <= target) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return (int) lo;
  }

  /**
   * Double sort key of the float with the given float sort key. Keys between that of positive infinity and
   * {@link #FLOAT_NAN_KEY} are NaNs, which widen to NaN and so map to {@link #DOUBLE_NAN_KEY}.
   */
  private static long widenedFloatKey(final int floatKey)
  {
    return doubleKey(floatFromKey(floatKey));
  }
}
