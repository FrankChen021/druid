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

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.NumericRangePredicateFactory;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.FloatVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * SIMD specialization of the equality and range matchers built by {@link FloatVectorValueMatcher}. When the mask
 * selects every row of the current vector, lanes are compared and the matching row numbers are appended by iterating
 * the bits of the resulting {@link VectorMask}. Null rows are folded in with the null vector as a {@link VectorMask}.
 *
 * Equality uses IEEE 754 lane comparison, the same as the scalar {@code ==} (NaN never matches, {@code -0.0} matches
 * {@code 0.0}). Ranges use {@link Double#compare} ordering on the value widened to double, like the scalar
 * predicate; since widening is monotonic, the double bounds are converted up front to bounds on {@link Float#compare}
 * sort keys, see {@link SimdMatcherSupport}.
 *
 * Sparse masks are delegated to the scalar matcher.
 */
public final class SimdFloatVectorValueMatcher extends BaseVectorValueMatcher
{
  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Integer> KEY_SPECIES = SPECIES.withLanes(int.class);

  private final VectorValueSelector selector;
  private final VectorValueMatcher scalarMatcher;
  private final boolean equality;
  private final float value;
  private final int lowerKeyInclusive;
  private final int upperKeyInclusive;
  private final DruidPredicateMatch nullMatch;
  private final VectorMatch match;

  private SimdFloatVectorValueMatcher(
      final VectorValueSelector selector,
      final VectorValueMatcher scalarMatcher,
      final boolean equality,
      final float value,
      final int lowerKeyInclusive,
      final int upperKeyInclusive,
      final DruidPredicateMatch nullMatch
  )
  {
    super(selector);
    this.selector = selector;
    this.scalarMatcher = scalarMatcher;
    this.equality = equality;
    this.value = value;
    this.lowerKeyInclusive = lowerKeyInclusive;
    this.upperKeyInclusive = upperKeyInclusive;
    this.nullMatch = nullMatch;
    this.match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);
  }

  /**
   * Matches rows {@code ==} to {@code value}; null rows are unknown.
   *
   * @param scalarMatcher equivalent scalar matcher, used for sparse masks
   */
  public static SimdFloatVectorValueMatcher forEquality(
      final VectorValueSelector selector,
      final float value,
      final VectorValueMatcher scalarMatcher
  )
  {
    return new SimdFloatVectorValueMatcher(selector, scalarMatcher, true, value, 0, 0, DruidPredicateMatch.UNKNOWN);
  }

  /**
   * Matches rows within {@code range}; null rows match according to {@code nullMatch}.
   *
   * @param scalarMatcher equivalent scalar matcher, used for sparse masks
   */
  public static SimdFloatVectorValueMatcher forRange(
      final VectorValueSelector selector,
      final NumericRangePredicateFactory.DoubleRange range,
      final DruidPredicateMatch nullMatch,
      final VectorValueMatcher scalarMatcher
  )
  {
    return new SimdFloatVectorValueMatcher(
        selector,
        scalarMatcher,
        false,
        0,
        SimdMatcherSupport.floatLowerKeyInclusive(range),
        SimdMatcherSupport.floatUpperKeyInclusive(range),
        nullMatch
    );
  }

  @Override
  public ReadableVectorMatch match(final ReadableVectorMatch mask, final boolean includeUnknown)
  {
    final int size = selector.getCurrentVectorSize();
    if (!mask.isAllTrue(size)) {
      return scalarMatcher.match(mask, includeUnknown);
    }

    final float[] vector = selector.getFloatVector();
    final boolean[] nulls = selector.getNullVector();
    final boolean nullMatches = nullMatch.matches(includeUnknown);
    final int numRows = equality
                        ? matchEquality(vector, nulls, nullMatches, size)
                        : matchRange(vector, nulls, nullMatches, size);
    match.setSelectionSize(numRows);
    return match;
  }

  private int matchEquality(final float[] vector, final boolean[] nulls, final boolean nullMatches, final int size)
  {
    final int[] selection = match.getSelection();
    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(size);
    int numRows = 0;
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      VectorMask<Float> matches = FloatVector.fromArray(SPECIES, vector, i).compare(VectorOperators.EQ, value);
      if (nulls != null) {
        final VectorMask<Float> isNull = VectorMask.fromArray(SPECIES, nulls, i);
        matches = nullMatches ? matches.or(isNull) : matches.andNot(isNull);
      }
      numRows = SimdMatcherSupport.appendMatches(selection, numRows, i, matches.toLong());
    }
    for (; i < size; i++) {
      if (nulls != null && nulls[i]) {
        if (nullMatches) {
          selection[numRows++] = i;
        }
      } else if (vector[i] == value) {
        selection[numRows++] = i;
      }
    }
    return numRows;
  }

  private int matchRange(final float[] vector, final boolean[] nulls, final boolean nullMatches, final int size)
  {
    final int[] selection = match.getSelection();
    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(size);
    final IntVector lower = IntVector.broadcast(KEY_SPECIES, lowerKeyInclusive);
    final IntVector upper = IntVector.broadcast(KEY_SPECIES, upperKeyInclusive);
    int numRows = 0;
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      final FloatVector v = FloatVector.fromArray(SPECIES, vector, i);
      final IntVector bits = v.viewAsIntegralLanes();
      final IntVector keys = bits.lanewise(
          VectorOperators.XOR,
          bits.lanewise(VectorOperators.ASHR, 31).lanewise(VectorOperators.AND, Integer.MAX_VALUE)
      ).blend(SimdMatcherSupport.FLOAT_NAN_KEY, v.test(VectorOperators.IS_NAN).cast(KEY_SPECIES));
      VectorMask<Integer> matches =
          keys.compare(VectorOperators.GE, lower).and(keys.compare(VectorOperators.LE, upper));
      if (nulls != null) {
        final VectorMask<Integer> isNull = VectorMask.fromArray(KEY_SPECIES, nulls, i);
        matches = nullMatches ? matches.or(isNull) : matches.andNot(isNull);
      }
      numRows = SimdMatcherSupport.appendMatches(selection, numRows, i, matches.toLong());
    }
    for (; i < size; i++) {
      if (nulls != null && nulls[i]) {
        if (nullMatches) {
          selection[numRows++] = i;
        }
      } else {
        final int key = SimdMatcherSupport.floatKey(vector[i]);
        if (key >= lowerKeyInclusive && key <= upperKeyInclusive) {
          selection[numRows++] = i;
        }
      }
    }
    return numRows;
  }
}
