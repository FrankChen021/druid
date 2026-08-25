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

import java.util.Objects;

/** Immutable description of a minimum/preferred query-memory request. */
public record MemoryRequest(long minimumBytes, long preferredBytes, MemoryPurpose purpose)
{
  public MemoryRequest
  {
    if (minimumBytes < 0) {
      throw new IllegalArgumentException("minimumBytes must not be negative");
    }
    if (preferredBytes < minimumBytes) {
      throw new IllegalArgumentException("preferredBytes must be greater than or equal to minimumBytes");
    }
    Objects.requireNonNull(purpose, "purpose");
  }
}
