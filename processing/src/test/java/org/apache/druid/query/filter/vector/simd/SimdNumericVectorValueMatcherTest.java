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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.NumericRangePredicateFactory.DoubleRange;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Builds {@link EqualityFilter} and {@link RangeFilter} vector matchers on long, double, and float columns twice,
 * once with {@link ExpressionProcessing#useVectorApi()} off (scalar) and once with it on (SIMD), and asserts that both
 * produce the same selection for every combination of vector size, data pattern, null pattern, mask, and
 * {@code includeUnknown}.
 *
 * Vector sizes cover tail-only, exact multiples of common lane counts, ragged tails, and a current vector size smaller
 * than the max vector size. Floating point data includes NaN (with both sign bits), -0.0, 0.0, infinities, and
 * extreme magnitudes, and filter bounds include the same special values, so that the SIMD sort-key comparisons are
 * checked against the scalar {@code ==} and {@link Double#compare} semantics.
 */
public class SimdNumericVectorValueMatcherTest extends InitializedNullHandlingTest
{
  private static final String COLUMN = "x";

  /**
   * Pairs of (max vector size, current vector size).
   */
  private static final int[][] VECTOR_SIZES = {{1, 1}, {7, 7}, {8, 8}, {17, 17}, {64, 64}, {512, 300}, {1023, 1023}};

  // largest and smallest positive values, spelled as bits because checkstyle forbids the MAX_VALUE/MIN_VALUE constants
  private static final double LARGEST_DOUBLE = Double.longBitsToDouble(0x7fefffffffffffffL);
  private static final double SMALLEST_DOUBLE = Double.longBitsToDouble(1L);
  private static final float LARGEST_FLOAT = Float.intBitsToFloat(0x7f7fffff);
  private static final float SMALLEST_FLOAT = Float.intBitsToFloat(1);

  private static final double NEGATIVE_NAN = Double.longBitsToDouble(0xfff8000000000001L);
  private static final float NEGATIVE_NAN_FLOAT = Float.intBitsToFloat(0xffc00001);

  private static final double[] SPECIAL_DOUBLES = {
      Double.NaN, NEGATIVE_NAN, -0.0, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, LARGEST_DOUBLE,
      -LARGEST_DOUBLE, SMALLEST_DOUBLE, -SMALLEST_DOUBLE, 1.0, -1.0, 0.1, 2.5, -2.5
  };

  private static final float[] SPECIAL_FLOATS = {
      Float.NaN, NEGATIVE_NAN_FLOAT, -0.0f, 0.0f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, LARGEST_FLOAT,
      -LARGEST_FLOAT, SMALLEST_FLOAT, -SMALLEST_FLOAT, 1.0f, -1.0f, 0.1f, 2.5f, -2.5f
  };

  private static final long[] SPECIAL_LONGS = {Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE - 1};

  /**
   * Filters that are decided without looking at the values, and so use the same constant matcher regardless of the
   * flag. Every other filter must get a SIMD matcher.
   */
  private final Set<Filter> constantFilters = new HashSet<>();

  @AfterEach
  public void tearDown()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Test
  public void testLong()
  {
    final List<Filter> filters = new ArrayList<>();
    for (long v : new long[]{-3, 0, 2, 100, Long.MIN_VALUE, Long.MAX_VALUE}) {
      filters.add(new EqualityFilter(COLUMN, ColumnType.LONG, v, null));
    }
    filters.add(new EqualityFilter(COLUMN, ColumnType.DOUBLE, 2.0, null));
    // non-integral value on a long column never matches
    final Filter nonIntegralEquality = new EqualityFilter(COLUMN, ColumnType.DOUBLE, 2.5, null);
    filters.add(nonIntegralEquality);
    constantFilters.add(nonIntegralEquality);
    final Object[] bounds = {
        null, -3L, -1L, 0L, 2L, 3L, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE - 1
    };
    addRangeFilters(filters, ColumnType.LONG, bounds);
    addRangeFilters(filters, ColumnType.DOUBLE, new Object[]{null, -2.5, -1.0, 0.0, 0.5, 1.5, 3.0});

    for (Filter filter : filters) {
      for (DataPattern dataPattern : DataPattern.values()) {
        for (int[] sizes : VECTOR_SIZES) {
          final long[] data = makeLongs(dataPattern, sizes[0]);
          for (NullPattern nullPattern : NullPattern.values()) {
            final boolean[] nulls = nullPattern.make(sizes[0]);
            final FakeSelector selector = new FakeSelector(sizes[0], sizes[1], data, null, null, nulls);
            assertSameResults(filter, ColumnType.LONG, selector, SimdLongVectorValueMatcher.class);
          }
        }
      }
    }
  }

  @Test
  public void testDouble()
  {
    final List<Filter> filters = new ArrayList<>();
    for (double v : SPECIAL_DOUBLES) {
      filters.add(new EqualityFilter(COLUMN, ColumnType.DOUBLE, v, null));
    }
    filters.add(new EqualityFilter(COLUMN, ColumnType.LONG, 2L, null));
    final List<Object> bounds = new ArrayList<>();
    bounds.add(null);
    for (double v : SPECIAL_DOUBLES) {
      bounds.add(v);
    }
    addRangeFilters(filters, ColumnType.DOUBLE, bounds.toArray());
    addRangeFilters(filters, ColumnType.LONG, new Object[]{null, -2L, 0L, 3L});

    for (Filter filter : filters) {
      for (DataPattern dataPattern : DataPattern.values()) {
        for (int[] sizes : VECTOR_SIZES) {
          final double[] data = makeDoubles(dataPattern, sizes[0]);
          for (NullPattern nullPattern : NullPattern.values()) {
            final boolean[] nulls = nullPattern.make(sizes[0]);
            final FakeSelector selector = new FakeSelector(sizes[0], sizes[1], null, data, null, nulls);
            assertSameResults(filter, ColumnType.DOUBLE, selector, SimdDoubleVectorValueMatcher.class);
          }
        }
      }
    }
  }

  @Test
  public void testFloat()
  {
    final List<Filter> filters = new ArrayList<>();
    for (float v : SPECIAL_FLOATS) {
      filters.add(new EqualityFilter(COLUMN, ColumnType.FLOAT, v, null));
    }
    filters.add(new EqualityFilter(COLUMN, ColumnType.DOUBLE, 0.1, null));
    filters.add(new EqualityFilter(COLUMN, ColumnType.LONG, 2L, null));
    final List<Object> bounds = new ArrayList<>();
    bounds.add(null);
    for (double v : SPECIAL_DOUBLES) {
      bounds.add(v);
    }
    // doubles that are not exactly representable as floats, and float values widened to double
    bounds.add(0.1);
    bounds.add((double) 0.1f);
    bounds.add(Math.nextUp((double) 0.1f));
    bounds.add(Math.nextDown((double) 0.1f));
    bounds.add((double) LARGEST_FLOAT * 2);
    bounds.add(SMALLEST_DOUBLE);
    addRangeFilters(filters, ColumnType.DOUBLE, bounds.toArray());
    addRangeFilters(filters, ColumnType.LONG, new Object[]{null, -2L, 0L, 3L});

    for (Filter filter : filters) {
      for (DataPattern dataPattern : DataPattern.values()) {
        for (int[] sizes : VECTOR_SIZES) {
          final float[] data = makeFloats(dataPattern, sizes[0]);
          for (NullPattern nullPattern : NullPattern.values()) {
            final boolean[] nulls = nullPattern.make(sizes[0]);
            final FakeSelector selector = new FakeSelector(sizes[0], sizes[1], null, null, data, nulls);
            assertSameResults(filter, ColumnType.FLOAT, selector, SimdFloatVectorValueMatcher.class);
          }
        }
      }
    }
  }

  @Test
  public void testFloatKeyBounds()
  {
    // exhaustively compare the precomputed float key bounds against Double.compare on widened floats, for a sample
    // of floats around each special bound
    final double[] bounds = {
        0.1, (double) 0.1f, Math.nextUp((double) 0.1f), -0.0, 0.0, Double.NaN, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, LARGEST_DOUBLE, -LARGEST_DOUBLE, SMALLEST_DOUBLE, 1e-50, -1e-50
    };
    for (double bound : bounds) {
      for (boolean open : new boolean[]{true, false}) {
        final DoubleRange lowerRange = new DoubleRange(true, open, bound, false, false, 0);
        final DoubleRange upperRange = new DoubleRange(false, false, 0, true, open, bound);
        final int lowerKey = SimdMatcherSupport.floatLowerKeyInclusive(lowerRange);
        final int upperKey = SimdMatcherSupport.floatUpperKeyInclusive(upperRange);
        for (float f : SPECIAL_FLOATS) {
          for (float g : new float[]{f, Math.nextUp(f), Math.nextDown(f), (float) bound, Math.nextUp((float) bound),
                                     Math.nextDown((float) bound)}) {
            final int key = SimdMatcherSupport.floatKey(g);
            final int cmp = Double.compare(g, bound);
            final String message = StringUtils.format("value[%s] bound[%s] open[%s]", g, bound, open);
            Assertions.assertEquals(open ? cmp > 0 : cmp >= 0, key >= lowerKey, message);
            Assertions.assertEquals(open ? cmp < 0 : cmp <= 0, key <= upperKey, message);
          }
        }
      }
    }
  }

  private static void addRangeFilters(
      final List<Filter> filters,
      final ColumnType matchValueType,
      final Object[] bounds
  )
  {
    for (Object lower : bounds) {
      for (Object upper : bounds) {
        if (lower == null && upper == null) {
          continue;
        }
        for (boolean lowerOpen : new boolean[]{true, false}) {
          for (boolean upperOpen : new boolean[]{true, false}) {
            filters.add(new RangeFilter(COLUMN, matchValueType, lower, upper, lowerOpen, upperOpen, null));
          }
        }
      }
    }
  }

  private void assertSameResults(
      final Filter filter,
      final ColumnType columnType,
      final FakeSelector selector,
      final Class<?> expectedSimdClass
  )
  {
    final FakeSelectorFactory selectorFactory = new FakeSelectorFactory(selector, columnType);

    ExpressionProcessing.initializeForTests();
    final VectorValueMatcher scalarMatcher = filter.makeVectorMatcher(selectorFactory);
    ExpressionProcessing.initializeForVectorApiTests();
    final VectorValueMatcher simdMatcher = filter.makeVectorMatcher(selectorFactory);
    if (constantFilters.contains(filter)) {
      Assertions.assertSame(scalarMatcher.getClass(), simdMatcher.getClass(), filter.toString());
    } else {
      Assertions.assertInstanceOf(expectedSimdClass, simdMatcher, filter.toString());
    }

    final int size = selector.getCurrentVectorSize();
    final List<ReadableVectorMatch> masks = new ArrayList<>();
    masks.add(VectorMatch.allTrue(size));
    masks.add(VectorMatch.allFalse());
    final VectorMatch sparse = VectorMatch.wrap(new int[size]);
    int sparseSize = 0;
    for (int i = 0; i < size; i += 3) {
      sparse.getSelection()[sparseSize++] = i;
    }
    sparse.setSelectionSize(sparseSize);
    masks.add(sparse);

    for (ReadableVectorMatch mask : masks) {
      for (boolean includeUnknown : new boolean[]{true, false}) {
        final int[] expected = toArray(scalarMatcher.match(mask, includeUnknown));
        final ReadableVectorMatch actualMatch = simdMatcher.match(mask, includeUnknown);
        Assertions.assertTrue(actualMatch.isValid(mask));
        Assertions.assertArrayEquals(
            expected,
            toArray(actualMatch),
            StringUtils.format(
                "filter[%s] maxSize[%s] size[%s] mask[%s] includeUnknown[%s]",
                filter,
                selector.getMaxVectorSize(),
                size,
                mask,
                includeUnknown
            )
        );
      }
    }
  }

  private static int[] toArray(final ReadableVectorMatch match)
  {
    return Arrays.copyOf(match.getSelection(), match.getSelectionSize());
  }

  private static long[] makeLongs(final DataPattern pattern, final int size)
  {
    final Random random = new Random(size);
    final long[] values = new long[size];
    for (int i = 0; i < size; i++) {
      switch (pattern) {
        case CONSTANT:
          values[i] = 2;
          break;
        case SMALL_RANGE:
          values[i] = random.nextInt(7) - 3;
          break;
        default:
          values[i] = random.nextInt(4) == 0
                      ? SPECIAL_LONGS[random.nextInt(SPECIAL_LONGS.length)]
                      : random.nextLong();
      }
    }
    return values;
  }

  private static double[] makeDoubles(final DataPattern pattern, final int size)
  {
    final Random random = new Random(size);
    final double[] values = new double[size];
    for (int i = 0; i < size; i++) {
      switch (pattern) {
        case CONSTANT:
          values[i] = 2.0;
          break;
        case SMALL_RANGE:
          values[i] = (random.nextInt(13) - 6) / 2.0;
          break;
        default:
          values[i] = random.nextBoolean()
                      ? SPECIAL_DOUBLES[random.nextInt(SPECIAL_DOUBLES.length)]
                      : (random.nextDouble() - 0.5) * 10;
      }
    }
    return values;
  }

  private static float[] makeFloats(final DataPattern pattern, final int size)
  {
    final Random random = new Random(size);
    final float[] values = new float[size];
    for (int i = 0; i < size; i++) {
      switch (pattern) {
        case CONSTANT:
          values[i] = 2.0f;
          break;
        case SMALL_RANGE:
          values[i] = (random.nextInt(13) - 6) / 2.0f;
          break;
        default:
          values[i] = random.nextBoolean()
                      ? SPECIAL_FLOATS[random.nextInt(SPECIAL_FLOATS.length)]
                      : (random.nextFloat() - 0.5f) * 10;
      }
    }
    return values;
  }

  private enum DataPattern
  {
    /**
     * Every row has the same value, so filters match all or nothing.
     */
    CONSTANT,
    /**
     * Few distinct small values, so equality and range filters have intermediate selectivity.
     */
    SMALL_RANGE,
    /**
     * Mix of special values (extremes, NaN, signed zeros, infinities) and random values.
     */
    SPECIAL
  }

  private enum NullPattern
  {
    NONE,
    SOME,
    ALL;

    @Nullable
    boolean[] make(final int size)
    {
      if (this == NONE) {
        return null;
      }
      final Random random = new Random(size + 1);
      final boolean[] nulls = new boolean[size];
      for (int i = 0; i < size; i++) {
        nulls[i] = this == ALL || random.nextInt(10) == 0;
      }
      return nulls;
    }
  }

  private static final class FakeSelector implements VectorValueSelector, ReadableVectorInspector
  {
    private final int maxSize;
    private final int currentSize;
    @Nullable
    private final long[] longs;
    @Nullable
    private final double[] doubles;
    @Nullable
    private final float[] floats;
    @Nullable
    private final boolean[] nulls;

    private FakeSelector(
        final int maxSize,
        final int currentSize,
        @Nullable final long[] longs,
        @Nullable final double[] doubles,
        @Nullable final float[] floats,
        @Nullable final boolean[] nulls
    )
    {
      this.maxSize = maxSize;
      this.currentSize = currentSize;
      this.longs = longs;
      this.doubles = doubles;
      this.floats = floats;
      this.nulls = nulls;
    }

    @Override
    public long[] getLongVector()
    {
      return longs;
    }

    @Override
    public float[] getFloatVector()
    {
      return floats;
    }

    @Override
    public double[] getDoubleVector()
    {
      return doubles;
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return nulls;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentSize;
    }

    @Override
    public int getId()
    {
      return 0;
    }
  }

  private static final class FakeSelectorFactory implements VectorColumnSelectorFactory
  {
    private final FakeSelector selector;
    private final ColumnType columnType;

    private FakeSelectorFactory(final FakeSelector selector, final ColumnType columnType)
    {
      this.selector = selector;
      this.columnType = columnType;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return selector;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorValueSelector makeValueSelector(final String column)
    {
      return selector;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(final String column)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(columnType);
    }
  }
}
