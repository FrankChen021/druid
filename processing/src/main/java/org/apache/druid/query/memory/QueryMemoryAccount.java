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

import java.util.Optional;

/** Per-query view of the node-level query-memory manager. */
public interface QueryMemoryAccount extends AutoCloseable
{
  /** Attempts elastic growth without waiting for capacity. */
  Optional<MemoryLease> tryAcquire(long minimumBytes, long preferredBytes, MemoryPurpose purpose);

  /** Reserves mandatory memory, waiting in FIFO order up to {@code timeoutMillis}. */
  MemoryLease acquireMinimum(long bytes, MemoryPurpose purpose, long timeoutMillis);

  /** Registers a callback used for cooperative reclamation when a request cannot fit immediately. */
  void registerReclaimer(MemoryReclaimer reclaimer);

  /** Marks this account as cancelling and prevents new reservations while existing leases remain valid. */
  void cancel();

  boolean isCancelling();

  boolean isClosed();

  long currentBytes();

  long peakBytes();

  @Override
  void close();
}
