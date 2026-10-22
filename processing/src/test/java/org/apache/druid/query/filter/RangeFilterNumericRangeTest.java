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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks the contract of {@link NumericRangePredicateFactory} for {@link RangeFilter}: the ranges returned by
 * {@link NumericRangePredicateFactory#getLongRange()} and {@link NumericRangePredicateFactory#getDoubleRange()} must
 * accept exactly the non-null values accepted by the long, double, and float predicates, since the SIMD vector
 * matchers evaluate the ranges instead of the predicates.
 */
public class RangeFilterNumericRangeTest extends InitializedNullHandlingTest
{
  // largest and smallest positive values, spelled as bits because checkstyle forbids the MAX_VALUE/MIN_VALUE constants
  private static final double LARGEST_DOUBLE = Double.longBitsToDouble(0x7fefffffffffffffL);
  private static final double SMALLEST_DOUBLE = Double.longBitsToDouble(1L);
  private static final float LARGEST_FLOAT = Float.intBitsToFloat(0x7f7fffff);
  private static final float SMALLEST_FLOAT = Float.intBitsToFloat(1);

  private static final double[] SPECIAL_DOUBLES = {
      Double.NaN, Double.longBitsToDouble(0xfff8000000000001L), -0.0, 0.0, Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY, LARGEST_DOUBLE, -LARGEST_DOUBLE, SMALLEST_DOUBLE, -SMALLEST_DOUBLE, 0.1, (double) 0.1f,
      1.0, -1.0, 1.5, -1.5, 2.5, 1e30, (double) LARGEST_FLOAT * 2, 9.223372036854775807E18, -9.223372036854775808E18
  };

  private static final float[] SPECIAL_FLOATS = {
      Float.NaN, Float.intBitsToFloat(0xffc00001), -0.0f, 0.0f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
      LARGEST_FLOAT, -LARGEST_FLOAT, SMALLEST_FLOAT, -SMALLEST_FLOAT, 0.1f, 1.0f, -1.0f, 1.5f, -1.5f, 2.5f, 1e30f
  };

  private static final long[] SPECIAL_LONGS = {
      Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MAX_VALUE - 1, -2, -1, 0, 1, 2, 3
  };

  @Test
  public void testRangesMatchPredicates()
  {
    final List<Object> doubleBounds = new ArrayList<>();
    doubleBounds.add(null);
    for (double v : SPECIAL_DOUBLES) {
      doubleBounds.add(v);
    }
    final List<Object> longBounds = new ArrayList<>();
    longBounds.add(null);
    for (long v : SPECIAL_LONGS) {
      longBounds.add(v);
    }
    final List<Object> floatBounds = new ArrayList<>();
    floatBounds.add(null);
    for (float v : SPECIAL_FLOATS) {
      floatBounds.add(v);
    }

    int checked = 0;
    checked += checkAll(ColumnType.DOUBLE, doubleBounds);
    checked += checkAll(ColumnType.FLOAT, floatBounds);
    checked += checkAll(ColumnType.LONG, longBounds);
    Assertions.assertTrue(checked > 0);
  }

  @Test
  public void testNonNumericMatchValueTypeHasNoRanges()
  {
    final RangeFilter filter = new RangeFilter("x", ColumnType.STRING, "1", "2", false, false, null);
    final NumericRangePredicateFactory factory = (NumericRangePredicateFactory) filter.getPredicateFactory();
    Assertions.assertNull(factory.getLongRange());
    Assertions.assertNull(factory.getDoubleRange());
  }

  private static int checkAll(final ColumnType matchValueType, final List<Object> bounds)
  {
    int checked = 0;
    for (Object lower : bounds) {
      for (Object upper : bounds) {
        if (lower == null && upper == null) {
          continue;
        }
        for (boolean lowerOpen : new boolean[]{true, false}) {
          for (boolean upperOpen : new boolean[]{true, false}) {
            final RangeFilter filter;
            try {
              filter = new RangeFilter("x", matchValueType, lower, upper, lowerOpen, upperOpen, null);
            }
            catch (RuntimeException e) {
              // bound not valid for the match value type, e.g. NaN for LONG
              continue;
            }
            check(filter, lower, upper);
            checked++;
          }
        }
      }
    }
    return checked;
  }

  private static void check(final RangeFilter filter, final Object lower, final Object upper)
  {
    final NumericRangePredicateFactory factory = (NumericRangePredicateFactory) filter.getPredicateFactory();
    final NumericRangePredicateFactory.LongRange longRange = factory.getLongRange();
    final NumericRangePredicateFactory.DoubleRange doubleRange = factory.getDoubleRange();
    Assertions.assertNotNull(longRange, filter.toString());
    Assertions.assertNotNull(doubleRange, filter.toString());

    final DruidLongPredicate longPredicate = factory.makeLongPredicate();
    final DruidDoublePredicate doublePredicate = factory.makeDoublePredicate();
    final DruidFloatPredicate floatPredicate = factory.makeFloatPredicate();

    // sample values: the specials, the bounds themselves, and their neighbors
    final List<Double> doubles = new ArrayList<>();
    for (double v : SPECIAL_DOUBLES) {
      doubles.add(v);
    }
    final List<Long> longs = new ArrayList<>();
    for (long v : SPECIAL_LONGS) {
      longs.add(v);
    }
    for (Object bound : new Object[]{lower, upper}) {
      if (bound != null) {
        final double d = ((Number) bound).doubleValue();
        doubles.add(d);
        doubles.add(Math.nextUp(d));
        doubles.add(Math.nextDown(d));
        if (Double.isFinite(d)) {
          for (long l : new long[]{(long) Math.floor(d), (long) Math.ceil(d)}) {
            longs.add(l);
            longs.add(l - 1);
            longs.add(l + 1);
          }
        }
      }
    }
    final List<Float> floats = new ArrayList<>();
    for (float v : SPECIAL_FLOATS) {
      floats.add(v);
    }
    for (double d : doubles) {
      final float f = (float) d;
      floats.add(f);
      floats.add(Math.nextUp(f));
      floats.add(Math.nextDown(f));
    }

    for (long v : longs) {
      Assertions.assertEquals(
          predicateMatches(longPredicate.applyLong(v)),
          inLongRange(longRange, v),
          StringUtils.format("filter[%s] long value[%s]", filter, v)
      );
    }
    for (double v : doubles) {
      Assertions.assertEquals(
          predicateMatches(doublePredicate.applyDouble(v)),
          inDoubleRange(doubleRange, v),
          StringUtils.format("filter[%s] double value[%s]", filter, v)
      );
    }
    for (float v : floats) {
      Assertions.assertEquals(
          predicateMatches(floatPredicate.applyFloat(v)),
          inDoubleRange(doubleRange, v),
          StringUtils.format("filter[%s] float value[%s]", filter, v)
      );
    }
  }

  private static boolean predicateMatches(final DruidPredicateMatch match)
  {
    // non-null inputs must never be unknown, so that includeUnknown does not matter
    Assertions.assertNotEquals(DruidPredicateMatch.UNKNOWN, match);
    return match == DruidPredicateMatch.TRUE;
  }

  private static boolean inLongRange(final NumericRangePredicateFactory.LongRange range, final long v)
  {
    return !range.isEmpty() && v >= range.lowerInclusive() && v <= range.upperInclusive();
  }

  private static boolean inDoubleRange(final NumericRangePredicateFactory.DoubleRange range, final double v)
  {
    if (range.hasLower()) {
      final int cmp = Double.compare(v, range.lower());
      if (range.lowerOpen() ? cmp <= 0 : cmp < 0) {
        return false;
      }
    }
    if (range.hasUpper()) {
      final int cmp = Double.compare(v, range.upper());
      if (range.upperOpen() ? cmp >= 0 : cmp > 0) {
        return false;
      }
    }
    return true;
  }
}
