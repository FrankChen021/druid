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

package org.apache.druid.utils;

/**
 * A detected upper bound for the memory available to the Druid process.
 *
 * <p>The bound may come from a Linux cgroup, a process address-space limit, or the JVM operating-system view. A
 * value of {@link #UNKNOWN_BYTES} means that no usable limit was available and callers must choose an explicit
 * fallback.</p>
 */
public record ProcessMemoryLimit(long bytes, Source source)
{
  public static final long UNKNOWN_BYTES = -1L;

  public ProcessMemoryLimit
  {
    if (bytes < UNKNOWN_BYTES) {
      throw new IllegalArgumentException("Memory limit cannot be less than -1");
    }
    if (source == null) {
      throw new NullPointerException("source");
    }
  }

  public boolean isKnown()
  {
    return bytes > 0;
  }

  public enum Source
  {
    CGROUP_V2,
    CGROUP_V1,
    PROCESS_RLIMIT,
    OPERATING_SYSTEM,
    UNKNOWN
  }
}
