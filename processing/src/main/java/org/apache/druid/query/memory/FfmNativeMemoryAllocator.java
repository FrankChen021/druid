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

package org.apache.druid.query.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/** Allocates one explicitly closeable shared FFM arena per lease. */
public class FfmNativeMemoryAllocator implements NativeMemoryAllocator
{
  public static final long DEFAULT_ALIGNMENT = 64;

  @Override
  public NativeMemoryAllocation allocate(final long bytes)
  {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must not be negative");
    }

    if (bytes == 0) {
      return new EmptyNativeMemoryAllocation();
    }

    final Arena arena = Arena.ofShared();
    try {
      return new FfmNativeMemoryAllocation(arena, arena.allocate(bytes, DEFAULT_ALIGNMENT));
    }
    catch (RuntimeException | Error e) {
      arena.close();
      throw e;
    }
  }

  private static class FfmNativeMemoryAllocation implements NativeMemoryAllocation
  {
    private final Arena arena;
    private final MemorySegment segment;

    private FfmNativeMemoryAllocation(final Arena arena, final MemorySegment segment)
    {
      this.arena = arena;
      this.segment = segment;
    }

    @Override
    public MemorySegment segment()
    {
      return segment;
    }

    @Override
    public void close()
    {
      arena.close();
    }
  }

  private static class EmptyNativeMemoryAllocation implements NativeMemoryAllocation
  {
    @Override
    public MemorySegment segment()
    {
      return MemorySegment.NULL;
    }

    @Override
    public void close()
    {
      // There is no arena to close for a zero-byte reservation.
    }
  }
}
