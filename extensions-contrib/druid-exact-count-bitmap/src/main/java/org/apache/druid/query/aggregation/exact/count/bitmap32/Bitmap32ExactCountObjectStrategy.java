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

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class Bitmap32ExactCountObjectStrategy implements ObjectStrategy<Bitmap32>
{
  static final Bitmap32ExactCountObjectStrategy STRATEGY = new Bitmap32ExactCountObjectStrategy();

  @Override
  public Class<? extends Bitmap32> getClazz()
  {
    return RoaringBitmap32Counter.class;
  }

  @Nullable
  @Override
  public Bitmap32 fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    final ByteBuffer readOnlyBuf = buffer.asReadOnlyBuffer();

    if (readOnlyBuf.remaining() < numBytes) {
      throw new BufferUnderflowException();
    }

    if (numBytes == 0) {
      // null and empty counters are stored as no bytes
      return new RoaringBitmap32Counter();
    }

    // the counter reads from the buffer without copying, so it is only valid while the column the buffer belongs to is
    readOnlyBuf.limit(readOnlyBuf.position() + numBytes);
    return RoaringBitmap32Counter.fromByteBuffer(readOnlyBuf);
  }

  @Nullable
  @Override
  public byte[] toBytes(@Nullable Bitmap32 val)
  {
    if (val == null || val.getCardinality() == 0) {
      return new byte[0];
    }
    return val.toByteBuffer().array();
  }

  @Override
  public int compare(Bitmap32 o1, Bitmap32 o2)
  {
    return Bitmap32ExactCountAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
