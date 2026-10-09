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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class Bitmap32ExactCountBuildBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Bitmap32>> collectors = new IdentityHashMap<>();

  // one-entry cache of the last looked up collector, to skip two hash lookups per row for consecutive hits on a group
  private ByteBuffer lastBuffer;
  private int lastPosition;
  private Bitmap32 lastCollector;

  public Bitmap32ExactCountBuildBufferAggregator(BaseLongColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    clearLastCollector();
    createNewCollector(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final Bitmap32 bitmap32Counter = getOrCreateCollector(buf, position);
    if (!selector.isNull()) {
      bitmap32Counter.add(RoaringBitmap32Counter.toInt(selector.getLong()));
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getOrCreateCollector(buf, position);
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

  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    clearLastCollector();
    createNewCollector(newBuffer, newPosition);
    Bitmap32 collector = collectors.get(oldBuffer).get(oldPosition);
    putCollectors(newBuffer, newPosition, collector);
    Int2ObjectMap<Bitmap32> collectorMap = collectors.get(oldBuffer);
    if (collectorMap != null) {
      collectorMap.remove(oldPosition);
      if (collectorMap.isEmpty()) {
        collectors.remove(oldBuffer);
      }
    }
  }

  private void putCollectors(final ByteBuffer buffer, final int position, final Bitmap32 collector)
  {
    Int2ObjectMap<Bitmap32> map = collectors.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, collector);
  }

  private Bitmap32 getOrCreateCollector(ByteBuffer buf, int position)
  {
    if (buf == lastBuffer && position == lastPosition) {
      return lastCollector;
    }
    Int2ObjectMap<Bitmap32> collectMap = collectors.get(buf);
    Bitmap32 bitmap32Counter = collectMap != null ? collectMap.get(position) : null;
    if (bitmap32Counter == null) {
      bitmap32Counter = createNewCollector(buf, position);
    }
    lastBuffer = buf;
    lastPosition = position;
    lastCollector = bitmap32Counter;
    return bitmap32Counter;
  }

  private void clearLastCollector()
  {
    lastBuffer = null;
    lastCollector = null;
  }

  private Bitmap32 createNewCollector(ByteBuffer buf, int position)
  {
    Bitmap32 bitmap32Counter = new RoaringBitmap32Counter();
    Int2ObjectMap<Bitmap32> collectorMap = collectors.computeIfAbsent(buf, k -> new Int2ObjectOpenHashMap<>());
    collectorMap.put(position, bitmap32Counter);
    return bitmap32Counter;
  }
}
