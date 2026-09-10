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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BlockingPoolMergeMemoryBackingAllocator implements MergeMemoryBackingAllocator
{
  private final BlockingPool<ByteBuffer> pool;
  private final int allocationSize;

  public BlockingPoolMergeMemoryBackingAllocator(final BlockingPool<ByteBuffer> pool, final int allocationSize)
  {
    this.pool = pool;
    this.allocationSize = allocationSize;
  }

  @Override
  public int allocationSize()
  {
    return allocationSize;
  }

  @Override
  public Optional<List<ResourceHolder<ByteBuffer>>> acquire(final int count, final long timeoutMillis)
  {
    if (count > pool.maxSize()) {
      return Optional.empty();
    }
    return holders(timeoutMillis < 0 ? pool.takeBatch(count) : pool.takeBatch(count, timeoutMillis));
  }

  @Override
  public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
  {
    if (count > pool.maxSize()) {
      return Optional.empty();
    }
    return holders(pool.takeBatch(count, 0));
  }

  private static Optional<List<ResourceHolder<ByteBuffer>>> holders(
      final List<ReferenceCountingResourceHolder<ByteBuffer>> holders
  )
  {
    return holders.isEmpty() ? Optional.empty() : Optional.of(new ArrayList<>(holders));
  }
}
