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

package org.apache.druid.msq.sql;

/// Marks the synchronous planning call made by the authenticated remote-frame submission resource.
public final class LiveFramesRequestContext
{
  private static final ThreadLocal<Integer> DEPTH = ThreadLocal.withInitial(() -> 0);

  private LiveFramesRequestContext()
  {
    // No instantiation.
  }

  /// Enters the internal remote-frame submission scope until the returned handle is closed.
  public static Scope enter()
  {
    DEPTH.set(DEPTH.get() + 1);
    return new Scope();
  }

  /// Returns whether planning runs within the authenticated source API.
  public static boolean isActive()
  {
    return DEPTH.get() > 0;
  }

  /// Restores the previous scope after internal planning completes.
  public static final class Scope implements AutoCloseable
  {
    private boolean closed;

    private Scope()
    {
    }

    @Override
    public void close()
    {
      if (!closed) {
        final int depth = DEPTH.get() - 1;
        if (depth == 0) {
          DEPTH.remove();
        } else {
          DEPTH.set(depth);
        }
        closed = true;
      }
    }
  }
}
