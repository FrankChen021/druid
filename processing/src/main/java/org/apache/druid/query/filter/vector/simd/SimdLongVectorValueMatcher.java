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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.NumericRangePredicateFactory;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.LongVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * SIMD specialization of the equality and range matchers built by {@link LongVectorValueMatcher}. When the mask
 * selects every row of the current vector, lanes are compared with {@link LongVector#compare} and the matching row
 * numbers are appended by iterating the bits of the resulting {@link VectorMask}. Null rows are folded in with the
 * null vector as a {@link VectorMask}.
 *
 * Sparse masks are delegated to the scalar matcher.
 */
public final class SimdLongVectorValueMatcher extends BaseVectorValueMatcher
{
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  private final VectorValueSelector selector;
  private final VectorValueMatcher scalarMatcher;
  private final boolean equality;
  private final long lowerInclusive;
  private final long upperInclusive;
  private final DruidPredicateMatch nullMatch;
  private final VectorMatch match;

  private SimdLongVectorValueMatcher(
      final VectorValueSelector selector,
      final VectorValueMatcher scalarMatcher,
      final boolean equality,
      final long lowerInclusive,
      final long upperInclusive,
      final DruidPredicateMatch nullMatch
  )
  {
    super(selector);
    this.selector = selector;
    this.scalarMatcher = scalarMatcher;
    this.equality = equality;
    this.lowerInclusive = lowerInclusive;
    this.upperInclusive = upperInclusive;
    this.nullMatch = nullMatch;
    this.match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);
  }

  /**
   * Matches rows equal to {@code value}; null rows are unknown.
   *
   * @param scalarMatcher equivalent scalar matcher, used for sparse masks
   */
  public static SimdLongVectorValueMatcher forEquality(
      final VectorValueSelector selector,
      final long value,
      final VectorValueMatcher scalarMatcher
  )
  {
    return new SimdLongVectorValueMatcher(selector, scalarMatcher, true, value, value, DruidPredicateMatch.UNKNOWN);
  }

  /**
   * Matches rows within {@code range}; null rows match according to {@code nullMatch}.
   *
   * @param scalarMatcher equivalent scalar matcher, used for sparse masks
   */
  public static SimdLongVectorValueMatcher forRange(
      final VectorValueSelector selector,
      final NumericRangePredicateFactory.LongRange range,
      final DruidPredicateMatch nullMatch,
      final VectorValueMatcher scalarMatcher
  )
  {
    if (range.isEmpty()) {
      return new SimdLongVectorValueMatcher(selector, scalarMatcher, false, 1, 0, nullMatch);
    }
    return new SimdLongVectorValueMatcher(
        selector,
        scalarMatcher,
        false,
        range.lowerInclusive(),
        range.upperInclusive(),
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

    final long[] vector = selector.getLongVector();
    final boolean[] nulls = selector.getNullVector();
    final boolean nullMatches = nullMatch.matches(includeUnknown);
    final int[] selection = match.getSelection();

    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(size);
    final LongVector lower = LongVector.broadcast(SPECIES, lowerInclusive);
    final LongVector upper = LongVector.broadcast(SPECIES, upperInclusive);
    int numRows = 0;
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      final LongVector v = LongVector.fromArray(SPECIES, vector, i);
      VectorMask<Long> matches = equality
                                 ? v.compare(VectorOperators.EQ, lower)
                                 : v.compare(VectorOperators.GE, lower).and(v.compare(VectorOperators.LE, upper));
      if (nulls != null) {
        final VectorMask<Long> isNull = VectorMask.fromArray(SPECIES, nulls, i);
        matches = nullMatches ? matches.or(isNull) : matches.andNot(isNull);
      }
      numRows = SimdMatcherSupport.appendMatches(selection, numRows, i, matches.toLong());
    }
    for (; i < size; i++) {
      if (nulls != null && nulls[i]) {
        if (nullMatches) {
          selection[numRows++] = i;
        }
      } else if (vector[i] >= lowerInclusive && vector[i] <= upperInclusive) {
        selection[numRows++] = i;
      }
    }

    match.setSelectionSize(numRows);
    return match;
  }
}
