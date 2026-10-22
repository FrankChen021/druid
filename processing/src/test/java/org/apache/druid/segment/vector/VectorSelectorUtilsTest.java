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

package org.apache.druid.segment.vector;

import com.google.common.collect.Sets;
import org.apache.druid.collections.IntSetTestUtility;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.bitmap.WrappedBitSetBitmap;
import org.apache.druid.collections.bitmap.WrappedConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class VectorSelectorUtilsTest
{
  private static final Set<Integer> NULLS = IntSetTestUtility.getSetBits();
  private static final Set<Integer> NULLS_PATTERN = alternatngPattern(10, 12);

  @Test
  public void testBitSetNullVector()
  {
    final WrappedBitSetBitmap bitmap = new WrappedBitSetBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    final WrappedBitSetBitmap bitmap2 = new WrappedBitSetBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testConciseMutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    final WrappedConciseBitmap bitmap2 = new WrappedConciseBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testConciseImmutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap, NULLS);
    final ImmutableBitmap immutable = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap.getBitmap())
    );
    assertNullVector(immutable, NULLS);

    final WrappedConciseBitmap bitmap2 = new WrappedConciseBitmap();
    populate(bitmap2, NULLS_PATTERN);
    final ImmutableBitmap immutable2 = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap2.getBitmap())
    );
    assertNullVector(immutable2, NULLS_PATTERN);
  }

  @Test
  public void testRoaringMutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    WrappedRoaringBitmap bitmap2 = new WrappedRoaringBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testRoaringImmutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap.toImmutableBitmap(), NULLS);

    WrappedRoaringBitmap bitmap2 = new WrappedRoaringBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2.toImmutableBitmap(), NULLS_PATTERN);
  }

  public static void populate(MutableBitmap bitmap, Set<Integer> setBits)
  {
    for (int i : setBits) {
      bitmap.add(i);
    }
  }

  private static Set<Integer> alternatngPattern(int smallSize, int rowCount)
  {
    ArrayList<Integer> bits = new ArrayList<>();
    boolean flipped = true;
    for (int i = 0; i < rowCount; i++) {
      if (i > 0 && i % smallSize == 0) {
        flipped = !flipped;
      }
      if (flipped) {
        bits.add(i);
      }
    }
    return Sets.newTreeSet(bits);
  }

  private void assertNullVector(ImmutableBitmap bitmap, Set<Integer> nulls)
  {
    // test entire set in one vector
    PeekableIntIterator iterator = bitmap.peekableIterator();
    final int vectorSize = 32;
    final boolean[] nullVector = new boolean[vectorSize];
    ReadableVectorOffset someOffset = new NoFilterVectorOffset(vectorSize, 0, vectorSize);

    VectorSelectorUtils.populateNullVector(nullVector, someOffset, iterator);
    for (int i = 0; i < vectorSize; i++) {
      Assertions.assertEquals(nulls.contains(i), nullVector[i]);
    }

    // test entire set split into 4 chunks with smaller vectors
    iterator = bitmap.peekableIterator();
    final int smallerVectorSize = 8;
    boolean[] smallVector = null;
    for (int offset = 0; offset < smallerVectorSize * 4; offset += smallerVectorSize) {
      ReadableVectorOffset smallOffset = new NoFilterVectorOffset(smallerVectorSize, offset, offset + smallerVectorSize);
      smallVector = VectorSelectorUtils.populateNullVector(smallVector, smallOffset, iterator);
      for (int i = 0; i < smallerVectorSize; i++) {
        if (smallVector == null) {
          Assertions.assertFalse(nulls.contains(offset + i));
        } else {
          Assertions.assertEquals(nulls.contains(offset + i), smallVector[i]);
        }
      }
    }

    // a magical vector perfectly sized to the number of nulls with a bitmap vector offset of just the nulls
    iterator = bitmap.peekableIterator();
    ReadableVectorOffset allTheNulls = new BitmapVectorOffset(nulls.size(), bitmap, 0, 32);
    smallVector = VectorSelectorUtils.populateNullVector(smallVector, allTheNulls, iterator);
    for (int i = 0; i < nulls.size(); i++) {
      Assertions.assertTrue(smallVector[i]);
    }
  }

  private static final int NUM_ROWS = 2000;
  private static final int VECTOR_SIZE = 64;

  public static Stream<Arguments> comparisonParameters()
  {
    final List<Arguments> args = new ArrayList<>();
    for (final String bitmapType : new String[]{"roaring", "roaringMutable", "concise", "conciseMutable", "bitset"}) {
      for (final String nulls : new String[]{"0", "0.001", "0.05", "0.5", "0.95", "1", "runs", "lastRow"}) {
        args.add(Arguments.of(bitmapType, nulls));
      }
    }
    return args.stream();
  }

  /**
   * Compares {@link VectorSelectorUtils#populateNullVector} against {@link ImmutableBitmap#get} and against the
   * original row-at-a-time implementation ({@link #legacyPopulateNullVector}), including return value, contents
   * beyond the current vector size, and iterator state after each call.
   */
  @ParameterizedTest(name = "bitmap[{0}] nulls[{1}]")
  @MethodSource("comparisonParameters")
  public void testMatchesReference(final String bitmapType, final String nulls)
  {
    final Random random = new Random(bitmapType.hashCode() * 31L + nulls.hashCode());
    final ImmutableBitmap bitmap = makeBitmap(bitmapType, nulls, random);

    // contiguous: whole column, starting mid-column, starting near the end; each ends with a partial vector
    for (final int start : new int[]{0, 1, NUM_ROWS / 3, NUM_ROWS - VECTOR_SIZE - 7, NUM_ROWS - 1}) {
      assertSequence(bitmap, contiguousOffsets(start, NUM_ROWS, false));
    }
    // contiguous, descending vector order: callers must reset the iterator for each vector
    assertSequence(bitmap, contiguousOffsets(0, NUM_ROWS, true));

    // non-contiguous, at various selectivities
    for (final double selectivity : new double[]{0.9, 0.5, 0.1, 0.01}) {
      final int[] selected = selectRows(random, selectivity);
      if (selected.length > 0) {
        assertSequence(bitmap, nonContiguousOffsets(selected, false));
        assertSequence(bitmap, nonContiguousOffsets(selected, true));
      }
    }

    // real BitmapVectorOffset, which produces a mix of contiguous and non-contiguous vectors
    final MutableBitmap filter = new RoaringBitmapFactory().makeEmptyMutableBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % 700 < 300 || random.nextDouble() < 0.3) {
        filter.add(i);
      }
    }
    final BitmapVectorOffset bitmapOffset = new BitmapVectorOffset(
        VECTOR_SIZE,
        new RoaringBitmapFactory().makeImmutableBitmap(filter),
        0,
        NUM_ROWS
    );
    final List<ReadableVectorOffset> fromBitmapOffset = new ArrayList<>();
    for (; !bitmapOffset.isDone(); bitmapOffset.advance()) {
      final int size = bitmapOffset.getCurrentVectorSize();
      if (bitmapOffset.isContiguous()) {
        final int start = bitmapOffset.getStartOffset();
        fromBitmapOffset.add(new NoFilterVectorOffset(VECTOR_SIZE, start, start + size));
      } else {
        fromBitmapOffset.add(new ArrayVectorOffset(VECTOR_SIZE, Arrays.copyOf(bitmapOffset.getOffsets(), size)));
      }
    }
    assertSequence(bitmap, fromBitmapOffset);
  }

  @Test
  public void testAllocatesWhenNullVectorNotProvided()
  {
    final MutableBitmap mutable = new RoaringBitmapFactory().makeEmptyMutableBitmap();
    mutable.add(3);
    mutable.add(70);
    final ImmutableBitmap bitmap = new RoaringBitmapFactory().makeImmutableBitmap(mutable);

    final boolean[] contiguous =
        VectorSelectorUtils.populateNullVector(null, new NoFilterVectorOffset(16, 0, 10), bitmap.peekableIterator());
    Assertions.assertNotNull(contiguous);
    Assertions.assertEquals(16, contiguous.length);
    Assertions.assertTrue(contiguous[3]);

    final boolean[] nonContiguous = VectorSelectorUtils.populateNullVector(
        null,
        new ArrayVectorOffset(16, new int[]{1, 3, 5}),
        bitmap.peekableIterator()
    );
    Assertions.assertNotNull(nonContiguous);
    Assertions.assertEquals(16, nonContiguous.length);
    Assertions.assertArrayEquals(new boolean[]{false, true, false}, Arrays.copyOf(nonContiguous, 3));

    // no nulls at or after the vector start: returns null, even though there are nulls earlier in the bitmap
    Assertions.assertNull(
        VectorSelectorUtils.populateNullVector(
            new boolean[16],
            new NoFilterVectorOffset(16, 71, 80),
            bitmap.peekableIterator()
        )
    );
    Assertions.assertNull(
        VectorSelectorUtils.populateNullVector(
            new boolean[16],
            new ArrayVectorOffset(16, new int[]{71, 75}),
            bitmap.peekableIterator()
        )
    );

    // nulls after the vector, but none within it: returns an array of all false
    final boolean[] noNullsInVector = new boolean[16];
    Arrays.fill(noNullsInVector, true);
    Assertions.assertSame(
        noNullsInVector,
        VectorSelectorUtils.populateNullVector(
            noNullsInVector,
            new NoFilterVectorOffset(16, 10, 20),
            bitmap.peekableIterator()
        )
    );
    for (int i = 0; i < 10; i++) {
      Assertions.assertFalse(noNullsInVector[i]);
    }
    for (int i = 10; i < 16; i++) {
      Assertions.assertTrue(noNullsInVector[i]);
    }
  }

  /**
   * Runs a sequence of offsets against one shared iterator the way the column vector selectors do (resetting the
   * iterator when an offset starts before the end of the previous one), checking each call against the reference.
   */
  private static void assertSequence(final ImmutableBitmap bitmap, final List<ReadableVectorOffset> offsets)
  {
    PeekableIntIterator iterator = bitmap.peekableIterator();
    PeekableIntIterator legacyIterator = bitmap.peekableIterator();
    boolean[] nullVector = null;
    boolean[] legacyNullVector = null;
    int offsetMark = -1;

    for (final ReadableVectorOffset offset : offsets) {
      final int size = offset.getCurrentVectorSize();
      final int[] rows = new int[size];
      for (int i = 0; i < size; i++) {
        rows[i] = offset.isContiguous() ? offset.getStartOffset() + i : offset.getOffsets()[i];
      }
      if (rows[0] < offsetMark) {
        iterator = bitmap.peekableIterator();
        legacyIterator = bitmap.peekableIterator();
      }
      offsetMark = rows[size - 1] + 1;

      final boolean[] before = nullVector == null ? null : nullVector.clone();
      final boolean[] result = VectorSelectorUtils.populateNullVector(nullVector, offset, iterator);
      legacyNullVector = legacyPopulateNullVector(legacyNullVector, offset, legacyIterator);

      final String msg = "vector starting at row[" + rows[0] + "] size[" + size + "]";
      Assertions.assertEquals(legacyNullVector == null, result == null, msg);
      if (result == null) {
        for (final int row : rows) {
          Assertions.assertFalse(bitmap.get(row), msg);
        }
        if (before != null) {
          Assertions.assertArrayEquals(before, nullVector, msg);
        }
      } else {
        if (nullVector != null) {
          Assertions.assertSame(nullVector, result, msg);
        }
        Assertions.assertEquals(offset.getMaxVectorSize(), result.length, msg);
        for (int i = 0; i < size; i++) {
          Assertions.assertEquals(bitmap.get(rows[i]), result[i], msg + " row[" + rows[i] + "]");
        }
        if (before != null) {
          // contents beyond the current vector size are untouched
          Assertions.assertArrayEquals(
              Arrays.copyOfRange(before, size, before.length),
              Arrays.copyOfRange(result, size, result.length),
              msg
          );
        }
        nullVector = result;
        // scribble beyond the current size, so the next call's "untouched" check is meaningful
        for (int i = size; i < nullVector.length; i++) {
          nullVector[i] = (i & 1) == 0;
        }
      }

      // iterator is left in the same state as the original implementation
      Assertions.assertEquals(legacyIterator.hasNext(), iterator.hasNext(), msg);
      if (iterator.hasNext()) {
        Assertions.assertEquals(legacyIterator.peekNext(), iterator.peekNext(), msg);
      }
    }
  }

  private static ImmutableBitmap makeBitmap(final String bitmapType, final String nulls, final Random random)
  {
    final BitmapFactory factory =
        bitmapType.startsWith("roaring") ? new RoaringBitmapFactory() : new ConciseBitmapFactory();
    final MutableBitmap mutable =
        "bitset".equals(bitmapType) ? new WrappedBitSetBitmap() : factory.makeEmptyMutableBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      final boolean isNull;
      switch (nulls) {
        case "runs":
          isNull = (i / 150) % 2 == 1;
          break;
        case "lastRow":
          isNull = i == NUM_ROWS - 1;
          break;
        default:
          isNull = random.nextDouble() < Double.parseDouble(nulls);
      }
      if (isNull) {
        mutable.add(i);
      }
    }
    if ("roaring".equals(bitmapType) || "concise".equals(bitmapType)) {
      return factory.makeImmutableBitmap(mutable);
    }
    return mutable;
  }

  private static int[] selectRows(final Random random, final double selectivity)
  {
    return IntStream.range(0, NUM_ROWS).filter(i -> random.nextDouble() < selectivity).toArray();
  }

  private static List<ReadableVectorOffset> contiguousOffsets(final int start, final int end, final boolean descending)
  {
    final List<ReadableVectorOffset> offsets = new ArrayList<>();
    for (int s = start; s < end; s += VECTOR_SIZE) {
      offsets.add(new NoFilterVectorOffset(VECTOR_SIZE, s, Math.min(s + VECTOR_SIZE, end)));
    }
    if (descending) {
      Collections.reverse(offsets);
    }
    return offsets;
  }

  private static List<ReadableVectorOffset> nonContiguousOffsets(final int[] selected, final boolean descending)
  {
    final List<ReadableVectorOffset> offsets = new ArrayList<>();
    for (int s = 0; s < selected.length; s += VECTOR_SIZE) {
      final int end = Math.min(s + VECTOR_SIZE, selected.length);
      offsets.add(new ArrayVectorOffset(VECTOR_SIZE, Arrays.copyOfRange(selected, s, end)));
    }
    if (descending) {
      Collections.reverse(offsets);
    }
    return offsets;
  }

  /**
   * Original row-at-a-time implementation of {@link VectorSelectorUtils#populateNullVector}, copied verbatim, used as a
   * reference.
   */
  @Nullable
  private static boolean[] legacyPopulateNullVector(
      @Nullable final boolean[] nullVector,
      final ReadableVectorOffset offset,
      final PeekableIntIterator nullIterator
  )
  {
    if (!nullIterator.hasNext()) {
      return null;
    }

    final boolean[] retVal;

    if (nullVector != null) {
      retVal = nullVector;
    } else {
      retVal = new boolean[offset.getMaxVectorSize()];
    }

    if (offset.isContiguous()) {
      final int startOffset = offset.getStartOffset();
      nullIterator.advanceIfNeeded(startOffset);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = i + startOffset;
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    } else {
      final int[] currentOffsets = offset.getOffsets();
      nullIterator.advanceIfNeeded(currentOffsets[0]);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = currentOffsets[i];
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    }

    return retVal;
  }

  /**
   * Non-contiguous offset over a fixed, ascending array of rows. The array may be shorter than maxVectorSize.
   */
  private static class ArrayVectorOffset implements ReadableVectorOffset
  {
    private final int maxVectorSize;
    private final int[] offsets;
    private final int size;

    ArrayVectorOffset(final int maxVectorSize, final int[] rows)
    {
      this.maxVectorSize = maxVectorSize;
      this.offsets = Arrays.copyOf(rows, maxVectorSize);
      this.size = rows.length;
    }

    @Override
    public boolean isContiguous()
    {
      return false;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return size;
    }

    @Override
    public int getStartOffset()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getOffsets()
    {
      return offsets;
    }

    @Override
    public int getId()
    {
      return offsets[0];
    }
  }
}
