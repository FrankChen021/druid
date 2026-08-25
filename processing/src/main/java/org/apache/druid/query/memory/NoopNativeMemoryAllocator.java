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

import java.lang.foreign.MemorySegment;

/** Keeps ACCOUNTED and LEGACY modes accounting-only until their operators are migrated. */
class NoopNativeMemoryAllocator implements NativeMemoryAllocator
{
  @Override
  public NativeMemoryAllocation allocate(final long bytes)
  {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must not be negative");
    }
    return new NoopNativeMemoryAllocation();
  }

  private static class NoopNativeMemoryAllocation implements NativeMemoryAllocation
  {
    @Override
    public MemorySegment segment()
    {
      return MemorySegment.NULL;
    }

    @Override
    public void close()
    {
      // Accounting-only leases have no physical allocation to release.
    }
  }
}
