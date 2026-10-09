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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set of 64 bit values, kept as one 32 bit roaring bitmap per distinct high 32 bits.
 *
 * The serialized form is the one of {@link org.roaringbitmap.longlong.Roaring64NavigableMap} with its default signed
 * ordering: a boolean for signed ordering, the number of high parts as a big endian int, then for every high part its
 * value as a big endian int followed by the portable serialization of the 32 bit bitmap holding the low 32 bits.
 * Data written by earlier versions is read as is.
 *
 * A counter read from a segment is a view over the segment buffer. No bitmap data is copied or decoded until it is
 * needed, and merging only collects references to the bitmaps of each high part. The union of those is computed once
 * on demand and cached, instead of copying and OR-ing every value as it is folded in. A counter that is a view over a
 * buffer is only valid while that buffer stays valid and unmodified, which includes accumulators it was folded into.
 */
public class RoaringBitmap64Counter implements Bitmap64
{
  private static final int HEADER_BYTES = 1 + Integer.BYTES;

  /**
   * The bitmaps of one high part, which are OR-ed together on demand. The union is cached and extended with the
   * bitmaps added since it was computed. It is built by OR-ing one bitmap after the other, which keeps run containers
   * as they are, whereas a lazy union turns them into bitmap containers that are expensive to optimize again.
   */
  private static class Part
  {
    private final List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>(1);
    @Nullable
    private MutableRoaringBitmap writable;
    @Nullable
    private MutableRoaringBitmap union;
    // number of bitmaps that are included in the union
    private int unioned;

    void addBitmap(final ImmutableRoaringBitmap bitmap)
    {
      bitmaps.add(bitmap);
    }

    void add(final int low)
    {
      if (writable == null) {
        writable = new MutableRoaringBitmap();
        bitmaps.add(writable);
      }
      writable.add(low);
      // the writable bitmap changed, so the union has to be built again
      union = null;
    }

    long cardinality()
    {
      return bitmaps.size() == 1 ? bitmaps.get(0).getLongCardinality() : union().getLongCardinality();
    }

    ImmutableRoaringBitmap bitmap()
    {
      return bitmaps.size() == 1 ? bitmaps.get(0) : union();
    }

    private MutableRoaringBitmap union()
    {
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

  private static class Snapshot
  {
    private final IntArrayList highs = new IntArrayList();
    private final List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>();

    void add(final int high, final ImmutableRoaringBitmap bitmap)
    {
      highs.add(high);
      bitmaps.add(bitmap);
    }
  }

  // Exactly one of the two is set. A counter starts as a read only view of deserialized data (sourceHighs and
  // sourceBitmaps) and turns into parts when it is first modified.
  @Nullable
  private int[] sourceHighs;
  @Nullable
  private ImmutableRoaringBitmap[] sourceBitmaps;
  @Nullable
  private Int2ObjectOpenHashMap<Part> parts;

  // one-entry cache for add(), values added in a row mostly share their high part
  private int lastHigh;
  @Nullable
  private Part lastPart;

  public RoaringBitmap64Counter()
  {
    this.parts = new Int2ObjectOpenHashMap<>();
  }

  private RoaringBitmap64Counter(final int[] sourceHighs, final ImmutableRoaringBitmap[] sourceBitmaps)
  {
    this.sourceHighs = sourceHighs;
    this.sourceBitmaps = sourceBitmaps;
  }

  /**
   * Reads a counter from its serialized form, which is copied. Throws for empty or corrupt data.
   */
  public static RoaringBitmap64Counter fromBytes(final byte[] bytes)
  {
    // the counter keeps reading from its bytes, so copy them to be independent of what the caller does with the array
    return fromByteBuffer(ByteBuffer.wrap(bytes.clone()));
  }

  /**
   * Reads a counter from the remaining bytes of the buffer, without copying them. The returned counter reads from the
   * buffer for as long as it is in use, so the buffer must stay valid and unmodified until then. Does not modify the
   * position, limit or order of the buffer.
   */
  public static RoaringBitmap64Counter fromByteBuffer(final ByteBuffer buffer)
  {
    final ByteBuffer bytes = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    try {
      final int limit = bytes.limit();
      int position = bytes.position();
      // position + 0 is the boolean for signed ordering, which only affects the order of the high parts
      final int numHighs = bytes.getInt(position + 1);
      position += HEADER_BYTES;
      if (numHighs < 0 || numHighs > (limit - position) / Integer.BYTES) {
        throw new IllegalArgumentException("Invalid number of high parts [" + numHighs + "]");
      }

      final boolean signedOrder = bytes.get(bytes.position()) != 0;
      final int[] highs = new int[numHighs];
      final ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numHighs];
      for (int i = 0; i < numHighs; i++) {
        highs[i] = bytes.getInt(position);
        position += Integer.BYTES;
        final ByteBuffer bitmapBuffer = bytes.duplicate();
        bitmapBuffer.position(position);
        bitmaps[i] = new ImmutableRoaringBitmap(bitmapBuffer);
        position += bitmaps[i].serializedSizeInBytes();
        if (position > limit) {
          throw new IllegalArgumentException("Truncated bitmap for high part [" + highs[i] + "]");
        }
      }
      verifyHighsAreUnique(highs, signedOrder);
      return new RoaringBitmap64Counter(highs, bitmaps);
    }
    catch (RuntimeException e) {
      throw new RuntimeException("Failed to deserialize RoaringBitmap64Counter", e);
    }
  }

  /**
   * A serialized value has one bitmap per high part, in the order the value says it uses. Cardinalities are added up
   * per bitmap, so a repeated high part would be counted twice. Trailing bytes after the last bitmap are not an error,
   * older versions wrote some.
   */
  private static void verifyHighsAreUnique(final int[] highs, final boolean signedOrder)
  {
    boolean ordered = true;
    for (int i = 1; i < highs.length && ordered; i++) {
      ordered = (signedOrder ? Integer.compare(highs[i - 1], highs[i]) : Integer.compareUnsigned(highs[i - 1], highs[i]))
                < 0;
    }
    if (!ordered) {
      // not in the expected order, which is only fine if every high part is there once
      final IntOpenHashSet seen = new IntOpenHashSet(highs.length);
      for (int high : highs) {
        if (!seen.add(high)) {
          throw new IllegalArgumentException("High part [" + high + "] is present more than once");
        }
      }
    }
  }

  @Override
  public synchronized void add(final long value)
  {
    final int high = (int) (value >>> 32);
    Part part = lastPart;
    if (part == null || high != lastHigh) {
      final Int2ObjectOpenHashMap<Part> map = parts();
      part = map.get(high);
      if (part == null) {
        part = new Part();
        map.put(high, part);
      }
      lastHigh = high;
      lastPart = part;
    }
    part.add((int) value);
  }

  @Override
  public synchronized long getCardinality()
  {
    long cardinality = 0;
    if (parts == null) {
      for (ImmutableRoaringBitmap bitmap : sourceBitmaps) {
        cardinality += bitmap.getLongCardinality();
      }
    } else {
      for (Part part : parts.values()) {
        cardinality += part.cardinality();
      }
    }
    return cardinality;
  }

  @Override
  public Bitmap64 fold(@Nullable final Bitmap64 rhs)
  {
    if (rhs == null || rhs == this) {
      return this;
    }
    // take the snapshot under the lock of the other counter, then add it under the lock of this one, so that the locks
    // are never held together
    final Snapshot snapshot = ((RoaringBitmap64Counter) rhs).snapshot();
    synchronized (this) {
      final Int2ObjectOpenHashMap<Part> map = parts();
      for (int i = 0; i < snapshot.highs.size(); i++) {
        getOrCreatePart(map, snapshot.highs.getInt(i)).addBitmap(snapshot.bitmaps.get(i));
      }
    }
    return this;
  }

  /**
   * The bitmaps of this counter with their high part. A writable bitmap, which can still change, is copied, so the
   * result does not change when values are added to this counter afterwards.
   */
  private synchronized Snapshot snapshot()
  {
    final Snapshot snapshot = new Snapshot();
    if (parts == null) {
      for (int i = 0; i < sourceHighs.length; i++) {
        snapshot.add(sourceHighs[i], sourceBitmaps[i]);
      }
    } else {
      for (Int2ObjectMap.Entry<Part> entry : parts.int2ObjectEntrySet()) {
        final Part part = entry.getValue();
        for (ImmutableRoaringBitmap bitmap : part.bitmaps) {
          snapshot.add(entry.getIntKey(), bitmap == part.writable ? ((MutableRoaringBitmap) bitmap).clone() : bitmap);
        }
      }
    }
    return snapshot;
  }

  @Override
  public synchronized ByteBuffer toByteBuffer()
  {
    final int[] highs;
    final ImmutableRoaringBitmap[] bitmaps;
    if (parts == null) {
      highs = sourceHighs;
      bitmaps = sourceBitmaps;
    } else {
      highs = new int[parts.size()];
      bitmaps = new ImmutableRoaringBitmap[parts.size()];
      int i = 0;
      for (Int2ObjectMap.Entry<Part> entry : parts.int2ObjectEntrySet()) {
        highs[i] = entry.getIntKey();
        bitmaps[i] = entry.getValue().bitmap();
        i++;
      }
    }

    // order the non empty high parts as signed ints, which is the order of Roaring64NavigableMap with signed longs
    final long[] order = new long[highs.length];
    int count = 0;
    int size = HEADER_BYTES;
    for (int i = 0; i < highs.length; i++) {
      if (!bitmaps[i].isEmpty()) {
        if (bitmaps[i] instanceof MutableRoaringBitmap) {
          ((MutableRoaringBitmap) bitmaps[i]).runOptimize();
        }
        order[count++] = ((long) highs[i] << 32) | i;
        size += Integer.BYTES + bitmaps[i].serializedSizeInBytes();
      }
    }
    Arrays.sort(order, 0, count);

    // the bitmaps are serialized as little endian, the rest as big endian
    final ByteBuffer out = ByteBuffer.allocate(size);
    out.put((byte) 1);
    out.putInt(count);
    for (int j = 0; j < count; j++) {
      final int i = (int) order[j];
      out.putInt(highs[i]);
      final ByteBuffer bitmapBuffer = out.slice().order(ByteOrder.LITTLE_ENDIAN);
      bitmaps[i].serialize(bitmapBuffer);
      out.position(out.position() + bitmaps[i].serializedSizeInBytes());
    }
    out.rewind();
    return out;
  }

  private Int2ObjectOpenHashMap<Part> parts()
  {
    if (parts == null) {
      final Int2ObjectOpenHashMap<Part> map = new Int2ObjectOpenHashMap<>(Math.max(sourceHighs.length, 2));
      for (int i = 0; i < sourceHighs.length; i++) {
        getOrCreatePart(map, sourceHighs[i]).addBitmap(sourceBitmaps[i]);
      }
      parts = map;
      sourceHighs = null;
      sourceBitmaps = null;
    }
    return parts;
  }

  private static Part getOrCreatePart(final Int2ObjectOpenHashMap<Part> map, final int high)
  {
    Part part = map.get(high);
    if (part == null) {
      part = new Part();
      map.put(high, part);
    }
    return part;
  }
}
