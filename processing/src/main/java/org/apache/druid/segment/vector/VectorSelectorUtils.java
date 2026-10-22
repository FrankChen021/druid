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

import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.util.Arrays;

public class VectorSelectorUtils
{
  /**
   * Helper used by ColumnarLongs, ColumnarDoubles, etc. to populate null-flag vectors.
   *
   * Returns null if the iterator has no nulls at or after the first row of the current vector. Otherwise, sets
   * positions [0, currentVectorSize) of the returned array (which is nullVector, if non-null) and leaves the rest
   * untouched. The iterator is only moved forward, and is left positioned at the first null at or after the last row
   * of the current vector, so it can be reused for subsequent vectors with higher row numbers. Callers must reset the
   * iterator if the offset moves backwards.
   *
   * For contiguous offsets, the cost of this method is proportional to the number of nulls within the vector rather
   * than the number of rows. For non-contiguous offsets, rows are compared against a cached next-null position, and
   * the iterator is only called when a row passes it.
   */
  @Nullable
  public static boolean[] populateNullVector(
      @Nullable final boolean[] nullVector,
      final ReadableVectorOffset offset,
      final PeekableIntIterator nullIterator
  )
  {
    if (!nullIterator.hasNext()) {
      return null;
    }

    final int size = offset.getCurrentVectorSize();

    if (offset.isContiguous()) {
      final int startOffset = offset.getStartOffset();
      nullIterator.advanceIfNeeded(startOffset);
      if (!nullIterator.hasNext()) {
        return null;
      }

      final boolean[] retVal = nullVector != null ? nullVector : new boolean[offset.getMaxVectorSize()];
      Arrays.fill(retVal, 0, size, false);

      // Walk only the nulls within [startOffset, startOffset + size). Step with advanceIfNeeded(row + 1) rather than
      // next(), which benchmarks faster within long runs of nulls. Stop without moving past a null on the last row,
      // so the iterator ends up at the first null >= the last row.
      final int lastRow = startOffset + size - 1;
      int row = nullIterator.peekNext();
      while (row <= lastRow) {
        retVal[row - startOffset] = true;
        if (row == lastRow) {
          break;
        }
        nullIterator.advanceIfNeeded(row + 1);
        if (!nullIterator.hasNext()) {
          break;
        }
        row = nullIterator.peekNext();
      }
      return retVal;
    } else {
      final int[] currentOffsets = offset.getOffsets();
      nullIterator.advanceIfNeeded(currentOffsets[0]);
      if (!nullIterator.hasNext()) {
        return null;
      }

      final boolean[] retVal = nullVector != null ? nullVector : new boolean[offset.getMaxVectorSize()];

      // Cache the next null, and only call into the iterator once a row passes it. The iterator is only advanced up
      // to the current row, so it ends up at the first null >= the last offset.
      int nextNull = nullIterator.peekNext();
      for (int i = 0; i < size; i++) {
        final int row = currentOffsets[i];
        if (row > nextNull) {
          nullIterator.advanceIfNeeded(row);
          if (!nullIterator.hasNext()) {
            Arrays.fill(retVal, i, size, false);
            break;
          }
          nextNull = nullIterator.peekNext();
        }
        retVal[i] = row == nextNull;
      }
      return retVal;
    }
  }
}
