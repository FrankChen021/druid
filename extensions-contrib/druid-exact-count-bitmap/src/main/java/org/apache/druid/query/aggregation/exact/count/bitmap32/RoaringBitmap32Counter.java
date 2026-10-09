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

package org.apache.druid.query.aggregation.exact.count.bitmap32;

import org.apache.druid.java.util.common.IAE;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of 32 bit values backed by roaring bitmaps. The serialized form is the portable serialization of a roaring
 * bitmap.
 *
 * A counter read from a segment is a view over the segment buffer, and folding counters only collects the bitmaps of
 * all of them. The union of those is computed when the result is needed, by OR-ing one bitmap after the other, and
 * cached. Reads after more folds only OR the new bitmaps. A counter that is a view over a buffer is only valid while
 * that buffer stays valid and unmodified, which includes accumulators it was folded into.
 */
public class RoaringBitmap32Counter implements Bitmap32
{
  private final List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(1);
  @Nullable
  private MutableRoaringBitmap writable;
  @Nullable
  private MutableRoaringBitmap union;
  // number of bitmaps that are included in the union
  private int unioned;

  public RoaringBitmap32Counter()
  {
  }

  private RoaringBitmap32Counter(final ImmutableRoaringBitmap bitmap)
  {
    bitmaps.add(bitmap);
  }

  /**
   * Reads a counter from its serialized form, which is copied. Throws for empty or corrupt data.
   */
  public static RoaringBitmap32Counter fromBytes(final byte[] bytes)
  {
    // the counter keeps reading from its bytes, so copy them to be independent of what the caller does with the array
    return fromByteBuffer(ByteBuffer.wrap(bytes.clone()));
  }

  /**
   * Reads a counter from the remaining bytes of the buffer, without copying them. The returned counter reads from the
   * buffer for as long as it is in use, so the buffer must stay valid and unmodified until then. Does not modify the
   * position, limit or order of the buffer.
   */
  public static RoaringBitmap32Counter fromByteBuffer(final ByteBuffer buffer)
  {
    try {
      return new RoaringBitmap32Counter(new ImmutableRoaringBitmap(buffer.duplicate()));
    }
    catch (RuntimeException e) {
      throw new RuntimeException("Failed to deserialize RoaringBitmap32Counter", e);
    }
  }

  /**
   * Converts a value of a long column to the 32 bit value this counter stores.
   *
   * @throws IAE if the value does not fit in 32 bits
   */
  public static int toInt(final long value)
  {
    if (value != (int) value) {
      throw new IAE("Value [%s] does not fit in 32 bits, which is what bitmap32 exact count supports", value);
    }
    return (int) value;
  }

  @Override
  public synchronized void add(final int value)
  {
    if (writable == null) {
      writable = new MutableRoaringBitmap();
      bitmaps.add(writable);
    }
    writable.add(value);
    // the writable bitmap changed, so the union has to be built again
    union = null;
  }

  @Override
  public synchronized long getCardinality()
  {
    return bitmap().getLongCardinality();
  }

  @Override
  public Bitmap32 fold(@Nullable final Bitmap32 rhs)
  {
    if (rhs == null || rhs == this) {
      return this;
    }
    // take the snapshot under the lock of the other counter, then add it under the lock of this one, so that the locks
    // are never held together
    final List<ImmutableRoaringBitmap> snapshot = ((RoaringBitmap32Counter) rhs).snapshotBitmaps();
    synchronized (this) {
      bitmaps.addAll(snapshot);
    }
    return this;
  }

  /**
   * The bitmaps of this counter. A writable bitmap, which can still change, is copied, so the result does not change
   * when values are added to this counter afterwards.
   */
  private synchronized List<ImmutableRoaringBitmap> snapshotBitmaps()
  {
    final List<ImmutableRoaringBitmap> snapshot = new ArrayList<>(bitmaps.size());
    for (ImmutableRoaringBitmap bitmap : bitmaps) {
      snapshot.add(bitmap == writable ? ((MutableRoaringBitmap) bitmap).clone() : bitmap);
    }
    return snapshot;
  }

  @Override
  public synchronized ByteBuffer toByteBuffer()
  {
    final ImmutableRoaringBitmap bitmap = bitmap();
    if (bitmap instanceof MutableRoaringBitmap) {
      ((MutableRoaringBitmap) bitmap).runOptimize();
    }
    final ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    bitmap.serialize(buffer);
    buffer.rewind();
    return buffer;
  }

  /**
   * The bitmap with all values of this counter.
   */
  private ImmutableRoaringBitmap bitmap()
  {
    if (bitmaps.size() == 1) {
      return bitmaps.get(0);
    }
    if (union == null) {
      union = new MutableRoaringBitmap();
      unioned = 0;
    }
    for (; unioned < bitmaps.size(); unioned++) {
      union.or(bitmaps.get(unioned));
    }
    return union;
  }
}
