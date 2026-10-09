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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class Bitmap64ExactCountMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Bitmap64> selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Bitmap64>> counterCache = new IdentityHashMap<>();

  // one-entry cache of the last looked up counter, to skip two hash lookups per row for consecutive hits on a group
  private ByteBuffer lastBuffer;
  private int lastPosition;
  private Bitmap64 lastCounter;

  public Bitmap64ExactCountMergeBufferAggregator(ColumnValueSelector<Bitmap64> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    clearLastCounter();
    RoaringBitmap64Counter emptyCounter = new RoaringBitmap64Counter();
    addToCache(buf, position, emptyCounter);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object x = selector.getObject();
    if (x == null) {
      return;
    }
    final Bitmap64 bitmap64Counter = getCounter(buf, position);
    bitmap64Counter.fold((RoaringBitmap64Counter) x);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getCounter(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    clearLastCounter();
    counterCache.clear();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    clearLastCounter();
    Bitmap64 counter = counterCache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, counter);
    Int2ObjectMap<Bitmap64> counterMap = counterCache.get(oldBuffer);
    if (counterMap != null) {
      counterMap.remove(oldPosition);
      if (counterMap.isEmpty()) {
        counterCache.remove(oldBuffer);
      }
    }
  }

  private Bitmap64 getCounter(final ByteBuffer buf, final int position)
  {
    if (buf == lastBuffer && position == lastPosition) {
      return lastCounter;
    }
    final Bitmap64 counter = counterCache.get(buf).get(position);
    lastBuffer = buf;
    lastPosition = position;
    lastCounter = counter;
    return counter;
  }

  private void clearLastCounter()
  {
    lastBuffer = null;
    lastCounter = null;
  }

  private void addToCache(final ByteBuffer buffer, final int position, final Bitmap64 counter)
  {
    Int2ObjectMap<Bitmap64> map = counterCache.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, counter);
  }
}
