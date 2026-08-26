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

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryResourceId;

import javax.inject.Inject;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Node-level accounting and admission for query execution memory.
 *
 * <p>This class deliberately reserves bytes before an allocator creates a physical segment. That ordering makes the
 * configured node and per-query limits hard limits even while operators are being migrated from direct buffers to
 * FFM-backed memory.</p>
 */
@LazySingleton
public class QueryMemoryManager
{
  private static final Logger log = new Logger(QueryMemoryManager.class);

  private final long maxBytes;
  private final long maxPerQueryBytes;
  private final long defaultAllocationTimeoutMillis;
  private final QueryMemoryConfig.Mode mode;
  private final NativeMemoryAllocator allocator;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition capacityChanged = lock.newCondition();
  private final Map<String, AccountImpl> accounts = new HashMap<>();
  private final Deque<PendingRequest> pendingRequests = new ArrayDeque<>();

  private long currentBytes;
  private long peakBytes;

  @Inject
  public QueryMemoryManager(final QueryMemoryConfig config)
  {
    this(
        config.getMaxBytes(),
        config.getMaxPerQueryBytes(),
        config.getAllocationTimeout().toStandardDuration().getMillis(),
        config.getMode(),
        createAllocator(config.getMode())
    );
  }

  QueryMemoryManager(
      final long maxBytes,
      final long maxPerQueryBytes,
      final long defaultAllocationTimeoutMillis
  )
  {
    this(
        maxBytes,
        maxPerQueryBytes,
        defaultAllocationTimeoutMillis,
        QueryMemoryConfig.Mode.LEGACY,
        new NoopNativeMemoryAllocator()
    );
  }

  QueryMemoryManager(
      final long maxBytes,
      final long maxPerQueryBytes,
      final long defaultAllocationTimeoutMillis,
      final NativeMemoryAllocator allocator
  )
  {
    this(
        maxBytes,
        maxPerQueryBytes,
        defaultAllocationTimeoutMillis,
        allocator instanceof FfmNativeMemoryAllocator ? QueryMemoryConfig.Mode.FFM : QueryMemoryConfig.Mode.LEGACY,
        allocator
    );
  }

  QueryMemoryManager(
      final long maxBytes,
      final long maxPerQueryBytes,
      final long defaultAllocationTimeoutMillis,
      final QueryMemoryConfig.Mode mode,
      final NativeMemoryAllocator allocator
  )
  {
    if (maxBytes < 0) {
      throw new IllegalArgumentException("maxBytes must not be negative");
    }
    if (maxPerQueryBytes < 0 || maxPerQueryBytes > maxBytes) {
      throw new IllegalArgumentException("maxPerQueryBytes must be between zero and maxBytes");
    }
    if (defaultAllocationTimeoutMillis < 0) {
      throw new IllegalArgumentException("defaultAllocationTimeoutMillis must not be negative");
    }
    this.maxBytes = maxBytes;
    this.maxPerQueryBytes = maxPerQueryBytes;
    this.defaultAllocationTimeoutMillis = defaultAllocationTimeoutMillis;
    this.mode = Objects.requireNonNull(mode, "mode");
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  private static NativeMemoryAllocator createAllocator(final QueryMemoryConfig.Mode mode)
  {
    return mode == QueryMemoryConfig.Mode.FFM
           ? new FfmNativeMemoryAllocator()
           : new NoopNativeMemoryAllocator();
  }

  /** Opens the unique memory account for a query resource id. */
  public QueryMemoryAccount openAccount(final QueryResourceId queryResourceId)
  {
    return openAccount(Objects.requireNonNull(queryResourceId, "queryResourceId").toString());
  }

  /** Opens an account using a caller-provided unique identifier. */
  public QueryMemoryAccount openAccount(final String queryId)
  {
    if (queryId == null || queryId.isBlank()) {
      throw new IllegalArgumentException("queryId must not be blank");
    }

    lock.lock();
    try {
      if (accounts.containsKey(queryId)) {
        throw new IllegalStateException(StringUtils.format("Query memory account[%s] is already open", queryId));
      }
      final AccountImpl account = new AccountImpl(queryId);
      accounts.put(queryId, account);
      return account;
    }
    finally {
      lock.unlock();
    }
  }

  public long getMaxBytes()
  {
    return maxBytes;
  }

  public long getMaxPerQueryBytes()
  {
    return maxPerQueryBytes;
  }

  public long getCurrentBytes()
  {
    lock.lock();
    try {
      return currentBytes;
    }
    finally {
      lock.unlock();
    }
  }

  public long getAvailableBytes()
  {
    lock.lock();
    try {
      return availableNodeBytesLocked();
    }
    finally {
      lock.unlock();
    }
  }

  public long getPeakBytes()
  {
    lock.lock();
    try {
      return peakBytes;
    }
    finally {
      lock.unlock();
    }
  }

  public int getActiveAccountCount()
  {
    lock.lock();
    try {
      return accounts.size();
    }
    finally {
      lock.unlock();
    }
  }

  public int getPendingRequestCount()
  {
    lock.lock();
    try {
      return pendingRequests.size();
    }
    finally {
      lock.unlock();
    }
  }

  public long getDefaultAllocationTimeoutMillis()
  {
    return defaultAllocationTimeoutMillis;
  }

  public boolean isFfmMode()
  {
    return mode == QueryMemoryConfig.Mode.FFM;
  }

  private Optional<MemoryLease> tryAcquire(
      final AccountImpl account,
      final MemoryRequest request
  )
  {
    final ElasticReservation initialReservation = reserveElastic(account, request);
    if (initialReservation.lease != null) {
      return Optional.of(materialize(initialReservation.lease));
    }
    if (initialReservation.reclaimTarget <= 0) {
      return Optional.empty();
    }

    reclaim(account, initialReservation.reclaimTarget);

    final ElasticReservation retryReservation = reserveElastic(account, request);
    return retryReservation.lease == null
           ? Optional.empty()
           : Optional.of(materialize(retryReservation.lease));
  }

  private ElasticReservation reserveElastic(final AccountImpl account, final MemoryRequest request)
  {
    lock.lock();
    try {
      ensureCanAcquireLocked(account);
      if (request.minimumBytes() > maxBytes
          || request.minimumBytes() > maxPerQueryBytes
          || !pendingRequests.isEmpty()) {
        return new ElasticReservation(null, 0);
      }

      final MemoryLeaseImpl reservedLease = reserveLocked(account, request, true);
      final long reclaimTarget = reservedLease == null
                                 ? request.minimumBytes() - availableForAccountLocked(account)
                                 : 0;
      return new ElasticReservation(reservedLease, reclaimTarget);
    }
    finally {
      lock.unlock();
    }
  }

  private MemoryLease acquireMinimum(
      final AccountImpl account,
      final MemoryRequest request,
      final long timeoutMillis
  )
  {
    if (timeoutMillis < 0) {
      throw invalidRequest("timeoutMillis must not be negative");
    }
    if (request.minimumBytes() > maxPerQueryBytes) {
      throw new QueryMemoryException(
          QueryMemoryException.Reason.QUERY_LIMIT,
          StringUtils.format(
              "Query memory request[%,d] exceeds maxPerQuery[%,d] for query[%s]",
              request.minimumBytes(),
              maxPerQueryBytes,
              account.queryId
          )
      );
    }
    if (request.minimumBytes() > maxBytes) {
      throw new QueryMemoryException(
          QueryMemoryException.Reason.NODE_LIMIT,
          StringUtils.format(
              "Query memory request[%,d] exceeds node maxBytes[%,d]",
              request.minimumBytes(),
              maxBytes
          )
      );
    }

    final long deadlineNanos = deadlineNanos(timeoutMillis);
    final long reclaimTarget;
    final MemoryLeaseImpl immediateLease;
    lock.lock();
    try {
      ensureCanAcquireLocked(account);
      if (pendingRequests.isEmpty()) {
        immediateLease = reserveLocked(account, request, false);
      } else {
        immediateLease = null;
      }
      reclaimTarget = immediateLease == null
                      ? request.minimumBytes() - availableForAccountLocked(account)
                      : 0;
    }
    finally {
      lock.unlock();
    }

    if (immediateLease != null) {
      return materialize(immediateLease);
    }

    if (reclaimTarget > 0) {
      reclaim(account, reclaimTarget);
    }

    MemoryLeaseImpl acquiredLease = null;
    lock.lock();
    try {
      ensureCanAcquireLocked(account);
      if (pendingRequests.isEmpty()) {
        acquiredLease = reserveLocked(account, request, false);
      }

      if (acquiredLease == null) {
        final PendingRequest pending = new PendingRequest(account);
        pendingRequests.addLast(pending);
        try {
          while (acquiredLease == null) {
            ensureCanAcquireLocked(account);
            if (pendingRequests.peekFirst() == pending) {
              acquiredLease = reserveLocked(account, request, false);
              if (acquiredLease != null) {
                pendingRequests.removeFirst();
                capacityChanged.signalAll();
                break;
              }
            }

            final long remainingNanos = deadlineNanos == Long.MAX_VALUE
                                        ? Long.MAX_VALUE
                                        : deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
              throw new QueryMemoryException(
                  QueryMemoryException.Reason.TIMEOUT,
                  StringUtils.format(
                      "Timed out waiting for %,d bytes of query memory for query[%s]",
                      request.minimumBytes(),
                      account.queryId
                  )
              );
            }
            try {
              if (remainingNanos == Long.MAX_VALUE) {
                capacityChanged.await();
              } else {
                // Re-enter the loop after a timed wait so the deadline is evaluated consistently.
                if (capacityChanged.awaitNanos(remainingNanos) <= 0) {
                  continue;
                }
              }
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new QueryMemoryException(
                  QueryMemoryException.Reason.INTERRUPTED,
                  StringUtils.format("Interrupted while waiting for query memory for query[%s]", account.queryId)
              );
            }
          }
        }
        finally {
          pendingRequests.remove(pending);
          capacityChanged.signalAll();
        }
      }
    }
    finally {
      lock.unlock();
    }

    return materialize(acquiredLease);
  }

  private MemoryLease materialize(final MemoryLeaseImpl lease)
  {
    final NativeMemoryAllocation allocation;
    try {
      allocation = Objects.requireNonNull(allocator.allocate(lease.size), "allocator returned null");
    }
    catch (OutOfMemoryError | RuntimeException e) {
      lease.close();
      log.warn(e, "Unable to allocate %,d bytes of native query memory", lease.size);
      throw new QueryMemoryException(
          QueryMemoryException.Reason.PHYSICAL_ALLOCATION_FAILED,
          StringUtils.format("Unable to allocate %,d bytes of native query memory", lease.size)
      );
    }

    final boolean canceled;
    synchronized (lease) {
      lock.lock();
      try {
        canceled = lease.closed.get() || lease.account.cancelling;
        if (!canceled) {
          lease.allocation = allocation;
        }
      }
      finally {
        lock.unlock();
      }
    }
    if (canceled) {
      closeAllocationAfterCancellation(allocation);
      lease.close();
      throw new QueryMemoryException(
          QueryMemoryException.Reason.CANCELED,
          StringUtils.format("Query memory account[%s] was canceled during allocation", lease.account.queryId)
      );
    }
    return lease;
  }

  private void closeAllocationAfterCancellation(final NativeMemoryAllocation allocation)
  {
    try {
      allocation.close();
    }
    catch (RuntimeException | Error e) {
      log.warn(e, "Failed to close a canceled query-memory allocation");
    }
  }

  private void reclaim(final AccountImpl account, final long targetBytes)
  {
    final List<MemoryReclaimer> reclaimers;
    lock.lock();
    try {
      ensureCanAcquireLocked(account);
      reclaimers = List.copyOf(account.reclaimers);
    }
    finally {
      lock.unlock();
    }

    long reclaimedBytes = 0;
    for (final MemoryReclaimer reclaimer : reclaimers) {
      final long remaining = targetBytes - reclaimedBytes;
      if (remaining <= 0) {
        break;
      }
      try {
        final long reclaimed = reclaimer.reclaim(remaining);
        if (reclaimed > 0) {
          reclaimedBytes = reclaimed >= targetBytes - reclaimedBytes
                           ? targetBytes
                           : reclaimedBytes + reclaimed;
        }
      }
      catch (RuntimeException e) {
        log.warn(e, "Query memory reclaimer failed for query[%s]", account.queryId);
      }
    }
  }

  private MemoryLeaseImpl reserveLocked(
      final AccountImpl account,
      final MemoryRequest request,
      final boolean elastic
  )
  {
    if (elastic && !pendingRequests.isEmpty()) {
      return null;
    }

    final long available = availableForAccountLocked(account);
    final long allocation = Math.min(request.preferredBytes(), available);
    if (allocation < request.minimumBytes()) {
      return null;
    }

    final MemoryLeaseImpl lease = new MemoryLeaseImpl(account, allocation, request.purpose());
    account.leases.add(lease);
    currentBytes += allocation;
    account.currentBytes += allocation;
    account.peakBytes = Math.max(account.peakBytes, account.currentBytes);
    peakBytes = Math.max(peakBytes, currentBytes);
    return lease;
  }

  private long availableForAccountLocked(final AccountImpl account)
  {
    return Math.max(0, Math.min(availableNodeBytesLocked(), maxPerQueryBytes - account.currentBytes));
  }

  private long availableNodeBytesLocked()
  {
    return Math.max(0, maxBytes - currentBytes);
  }

  private void release(final MemoryLeaseImpl lease)
  {
    lock.lock();
    try {
      if (lease.account.leases.remove(lease)) {
        currentBytes -= lease.size;
        lease.account.currentBytes -= lease.size;
        capacityChanged.signalAll();
      }
    }
    finally {
      lock.unlock();
    }
  }

  private void cancel(final AccountImpl account)
  {
    lock.lock();
    try {
      if (!account.cancelling) {
        account.cancelling = true;
        removePendingRequestsLocked(account);
        capacityChanged.signalAll();
      }
    }
    finally {
      lock.unlock();
    }
  }

  private void close(final AccountImpl account)
  {
    final List<MemoryLeaseImpl> leases;
    lock.lock();
    try {
      if (account.closed) {
        return;
      }
      account.cancelling = true;
      account.closed = true;
      accounts.remove(account.queryId, account);
      removePendingRequestsLocked(account);
      leases = new ArrayList<>(account.leases);
      capacityChanged.signalAll();
    }
    finally {
      lock.unlock();
    }

    for (final MemoryLeaseImpl lease : leases) {
      lease.close();
    }
  }

  private void removePendingRequestsLocked(final AccountImpl account)
  {
    for (final Iterator<PendingRequest> iterator = pendingRequests.iterator(); iterator.hasNext();) {
      if (iterator.next().account == account) {
        iterator.remove();
      }
    }
  }

  private void ensureCanAcquireLocked(final AccountImpl account)
  {
    if (account.closed || account.cancelling) {
      throw new QueryMemoryException(
          QueryMemoryException.Reason.CANCELED,
          StringUtils.format("Query memory account[%s] is canceled", account.queryId)
      );
    }
  }

  private static QueryMemoryException invalidRequest(final String message)
  {
    return new QueryMemoryException(QueryMemoryException.Reason.INVALID_REQUEST, message);
  }

  private static MemoryRequest makeRequest(
      final long minimumBytes,
      final long preferredBytes,
      final MemoryPurpose purpose
  )
  {
    try {
      return new MemoryRequest(minimumBytes, preferredBytes, purpose);
    }
    catch (RuntimeException e) {
      throw invalidRequest(e.getMessage());
    }
  }

  private static long deadlineNanos(final long timeoutMillis)
  {
    if (timeoutMillis == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    final long now = System.nanoTime();
    return timeoutNanos == Long.MAX_VALUE
           || (now >= 0 && timeoutNanos > Long.MAX_VALUE - now)
           ? Long.MAX_VALUE
           : now + timeoutNanos;
  }

  private static class PendingRequest
  {
    private final AccountImpl account;

    private PendingRequest(final AccountImpl account)
    {
      this.account = account;
    }
  }

  private static class ElasticReservation
  {
    private final MemoryLeaseImpl lease;
    private final long reclaimTarget;

    private ElasticReservation(final MemoryLeaseImpl lease, final long reclaimTarget)
    {
      this.lease = lease;
      this.reclaimTarget = reclaimTarget;
    }
  }

  private class AccountImpl implements QueryMemoryAccount
  {
    private final String queryId;
    private final Set<MemoryLeaseImpl> leases = new HashSet<>();
    private final List<MemoryReclaimer> reclaimers = new ArrayList<>();
    private long currentBytes;
    private long peakBytes;
    private boolean cancelling;
    private boolean closed;

    private AccountImpl(final String queryId)
    {
      this.queryId = queryId;
    }

    @Override
    public Optional<MemoryLease> tryAcquire(
        final long minimumBytes,
        final long preferredBytes,
        final MemoryPurpose purpose
    )
    {
      return QueryMemoryManager.this.tryAcquire(this, makeRequest(minimumBytes, preferredBytes, purpose));
    }

    @Override
    public MemoryLease acquireMinimum(
        final long bytes,
        final MemoryPurpose purpose,
        final long timeoutMillis
    )
    {
      return QueryMemoryManager.this.acquireMinimum(this, makeRequest(bytes, bytes, purpose), timeoutMillis);
    }

    @Override
    public void registerReclaimer(final MemoryReclaimer reclaimer)
    {
      Objects.requireNonNull(reclaimer, "reclaimer");
      lock.lock();
      try {
        ensureCanAcquireLocked(this);
        reclaimers.add(reclaimer);
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public void cancel()
    {
      QueryMemoryManager.this.cancel(this);
    }

    @Override
    public boolean isCancelling()
    {
      lock.lock();
      try {
        return cancelling;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isClosed()
    {
      lock.lock();
      try {
        return closed;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public long currentBytes()
    {
      lock.lock();
      try {
        return currentBytes;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public long peakBytes()
    {
      lock.lock();
      try {
        return peakBytes;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public void close()
    {
      QueryMemoryManager.this.close(this);
    }
  }

  private class MemoryLeaseImpl implements MemoryLease
  {
    private final AccountImpl account;
    private final long size;
    private final MemoryPurpose purpose;
    private final AtomicBoolean closed = new AtomicBoolean();
    private NativeMemoryAllocation allocation;

    private MemoryLeaseImpl(final AccountImpl account, final long size, final MemoryPurpose purpose)
    {
      this.account = account;
      this.size = size;
      this.purpose = purpose;
    }

    @Override
    public MemorySegment segment()
    {
      synchronized (this) {
        return allocation == null ? MemorySegment.NULL : allocation.segment();
      }
    }

    @Override
    public long size()
    {
      return size;
    }

    @Override
    public MemoryPurpose purpose()
    {
      return purpose;
    }

    @Override
    public void close()
    {
      if (closed.compareAndSet(false, true)) {
        final NativeMemoryAllocation allocationToClose;
        synchronized (this) {
          allocationToClose = allocation;
          allocation = null;
        }
        try {
          if (allocationToClose != null) {
            allocationToClose.close();
          }
        }
        finally {
          release(this);
        }
      }
    }
  }
}
