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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.ResourceLimitExceededException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Page allocator backed by whole merge buffers. Backing buffers are returned as soon as all of their pages are free.
 */
public class MergeMemoryManager
{
  private final MergeMemoryBackingAllocator backingAllocator;
  private final int pageSize;
  private final int pagesPerBackingAllocation;
  private final ReentrantLock admissionLock = new ReentrantLock(true);
  private final Deque<PhysicalPage> freePages = new ArrayDeque<>();
  private final Set<BackingAllocation> backings = Collections.newSetFromMap(new IdentityHashMap<>());
  private int pendingMinimumAcquisitions;

  public MergeMemoryManager(final MergeMemoryBackingAllocator backingAllocator, final int pageSize)
  {
    if (pageSize < Long.BYTES || pageSize % Long.BYTES != 0) {
      throw new IAE("pageSize[%d] must be positive and divisible by [%d]", pageSize, Long.BYTES);
    }
    this.backingAllocator = backingAllocator;
    this.pageSize = pageSize;
    this.pagesPerBackingAllocation = backingAllocator.allocationSize() / pageSize;
    if (pagesPerBackingAllocation == 0) {
      throw new IAE(
          "Backing allocation size[%d] is smaller than page size[%d]",
          backingAllocator.allocationSize(),
          pageSize
      );
    }
  }

  public MergeMemoryLease acquireMinimum(
      final QueryResourceId queryResourceId,
      final int minimumPages,
      final int maximumPages,
      final long timeoutMillis
  )
  {
    if (minimumPages <= 0 || maximumPages < minimumPages) {
      throw new IAE(
          "Invalid page limits for query[%s]: minimum[%d], maximum[%d]",
          queryResourceId,
          minimumPages,
          maximumPages
      );
    }

    final long startNanos = System.nanoTime();
    synchronized (this) {
      pendingMinimumAcquisitions++;
    }
    boolean admissionLocked = false;
    try {
      try {
        if (timeoutMillis < 0) {
          admissionLock.lockInterruptibly();
          admissionLocked = true;
        } else {
          admissionLocked = admissionLock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS);
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (!admissionLocked) {
        throw capacityExceeded();
      }

      while (true) {
        final int backingCount;
        synchronized (this) {
          if (freePages.size() >= minimumPages) {
            return new Lease(queryResourceId, takePages(minimumPages), maximumPages);
          }
          backingCount = divideRoundUp(minimumPages - freePages.size(), pagesPerBackingAllocation);
        }

        final long remainingTimeoutMillis = timeoutMillis < 0
                                            ? -1
                                            : Math.max(
                                                0,
                                                timeoutMillis - TimeUnit.NANOSECONDS.toMillis(
                                                    System.nanoTime() - startNanos
                                                )
                                            );
        final Optional<List<ResourceHolder<ByteBuffer>>> allocations;
        try {
          allocations = backingAllocator.acquire(backingCount, remainingTimeoutMillis);
        }
        catch (Throwable t) {
          synchronized (this) {
            releaseCompletelyFreeBackings();
          }
          throw t;
        }
        synchronized (this) {
          if (!allocations.isPresent()) {
            releaseCompletelyFreeBackings();
            throw capacityExceeded();
          }
          try {
            addBackings(allocations.get());
            if (freePages.size() >= minimumPages) {
              return new Lease(queryResourceId, takePages(minimumPages), maximumPages);
            }
          }
          catch (Throwable t) {
            releaseCompletelyFreeBackings();
            throw t;
          }
        }
      }
    }
    finally {
      synchronized (this) {
        pendingMinimumAcquisitions--;
      }
      if (admissionLocked) {
        admissionLock.unlock();
      }
    }
  }

  public int pageSize()
  {
    return pageSize;
  }

  synchronized int getBackingAllocationCount()
  {
    return backings.size();
  }

  synchronized int getFreePageCount()
  {
    return freePages.size();
  }

  private synchronized Optional<List<PhysicalPage>> tryBorrowPages(final Lease owner, final int count)
  {
    if (pendingMinimumAcquisitions > 0) {
      return Optional.empty();
    }
    if (freePages.size() < count) {
      final int backingCount = divideRoundUp(count - freePages.size(), pagesPerBackingAllocation);
      final Optional<List<ResourceHolder<ByteBuffer>>> allocations = backingAllocator.tryAcquire(backingCount);
      if (!allocations.isPresent()) {
        return Optional.empty();
      }
      addBackings(allocations.get());
    }
    final List<PhysicalPage> pages = takePages(count);
    pages.forEach(page -> page.owner = owner);
    return Optional.of(pages);
  }

  private List<PhysicalPage> takePages(final int count)
  {
    final List<PhysicalPage> pages = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final PhysicalPage page = freePages.removeFirst();
      page.backing.assignedPages++;
      pages.add(page);
    }
    return pages;
  }

  private void addBacking(final ResourceHolder<ByteBuffer> holder)
  {
    final ByteBuffer buffer = holder.get().duplicate();
    buffer.clear();
    if (buffer.capacity() != backingAllocator.allocationSize()) {
      holder.close();
      throw new ISE(
          "Backing allocator declared size[%d] but returned buffer capacity[%d]",
          backingAllocator.allocationSize(),
          buffer.capacity()
      );
    }
    final int pageCount = buffer.capacity() / pageSize;
    if (pageCount == 0) {
      holder.close();
      throw new ResourceLimitExceededException(
          StringUtils.format("Merge buffer capacity[%d] is smaller than page size[%d]", buffer.capacity(), pageSize)
      );
    }
    final BackingAllocation backing = new BackingAllocation(holder);
    backings.add(backing);
    for (int pageNumber = 0; pageNumber < pageCount; pageNumber++) {
      final ByteBuffer pageBuffer = buffer.duplicate();
      pageBuffer.position(pageNumber * pageSize);
      pageBuffer.limit((pageNumber + 1) * pageSize);
      final PhysicalPage page = new PhysicalPage(backing, pageBuffer.slice());
      backing.pages.add(page);
      freePages.addLast(page);
    }
  }

  private void addBackings(final List<ResourceHolder<ByteBuffer>> holders)
  {
    for (int i = 0; i < holders.size(); i++) {
      try {
        addBacking(holders.get(i));
      }
      catch (Throwable t) {
        for (int remaining = i + 1; remaining < holders.size(); remaining++) {
          holders.get(remaining).close();
        }
        releaseCompletelyFreeBackings();
        throw t;
      }
    }
  }

  private synchronized void releasePage(final PhysicalPage page, final Lease owner)
  {
    if (page.owner != owner) {
      throw new ISE("Merge-memory page is not owned by query[%s]", owner.queryResourceId);
    }
    page.owner = null;
    page.backing.assignedPages--;
    freePages.addLast(page);
    if (page.backing.assignedPages == 0) {
      releaseBacking(page.backing);
    }
  }

  private void releaseCompletelyFreeBackings()
  {
    final List<BackingAllocation> candidates = new ArrayList<>(backings);
    candidates.stream().filter(backing -> backing.assignedPages == 0).forEach(this::releaseBacking);
  }

  private void releaseBacking(final BackingAllocation backing)
  {
    if (!backings.remove(backing)) {
      return;
    }
    freePages.removeAll(backing.pages);
    backing.holder.close();
  }

  private class Lease implements MergeMemoryLease
  {
    private final QueryResourceId queryResourceId;
    private final int maximumPages;
    private final Deque<PhysicalPage> guaranteedAvailable = new ArrayDeque<>();
    private final Set<PhysicalPage> guaranteedPages = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<PhysicalPage> borrowedPages = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<PageHandle, PhysicalPage> handles = new IdentityHashMap<>();
    private boolean closed;

    private Lease(
        final QueryResourceId queryResourceId,
        final List<PhysicalPage> guaranteedPages,
        final int maximumPages
    )
    {
      this.queryResourceId = queryResourceId;
      this.maximumPages = maximumPages;
      guaranteedPages.forEach(page -> {
        page.owner = this;
        this.guaranteedPages.add(page);
        guaranteedAvailable.addLast(page);
      });
    }

    @Override
    public synchronized Optional<List<MergeMemoryPage>> tryAcquirePages(final int count)
    {
      if (closed) {
        throw new ISE("Merge-memory lease for query[%s] is closed", queryResourceId);
      }
      if (count <= 0) {
        throw new IAE("Page count[%d] must be positive", count);
      }
      final int additionalPages = Math.max(0, count - guaranteedAvailable.size());
      if ((long) guaranteedPages.size() + borrowedPages.size() + additionalPages > maximumPages) {
        return Optional.empty();
      }

      final List<PhysicalPage> borrowed;
      if (additionalPages == 0) {
        borrowed = Collections.emptyList();
      } else {
        final Optional<List<PhysicalPage>> acquired = tryBorrowPages(this, additionalPages);
        if (!acquired.isPresent()) {
          return Optional.empty();
        }
        borrowed = acquired.get();
        borrowedPages.addAll(borrowed);
      }

      final List<MergeMemoryPage> result = new ArrayList<>(count);
      for (int i = 0; i < count - borrowed.size(); i++) {
        result.add(newHandle(guaranteedAvailable.removeFirst()));
      }
      borrowed.forEach(page -> result.add(newHandle(page)));
      return Optional.of(result);
    }

    @Override
    public int pageSize()
    {
      return pageSize;
    }

    @Override
    public synchronized void close()
    {
      if (closed) {
        return;
      }
      closed = true;
      handles.clear();
      final List<PhysicalPage> pages = new ArrayList<>(guaranteedPages.size() + borrowedPages.size());
      pages.addAll(guaranteedPages);
      pages.addAll(borrowedPages);
      guaranteedAvailable.clear();
      guaranteedPages.clear();
      borrowedPages.clear();
      pages.forEach(page -> releasePage(page, this));
    }

    private PageHandle newHandle(final PhysicalPage page)
    {
      final PageHandle handle = new PageHandle(this);
      handles.put(handle, page);
      return handle;
    }

    private synchronized ByteBuffer get(final PageHandle handle)
    {
      final PhysicalPage page = handles.get(handle);
      if (closed || page == null) {
        throw new ISE("Merge-memory page handle for query[%s] is closed", queryResourceId);
      }
      return page.buffer;
    }

    private synchronized void release(final PageHandle handle)
    {
      final PhysicalPage page = handles.remove(handle);
      if (page == null) {
        return;
      }
      if (guaranteedPages.contains(page)) {
        guaranteedAvailable.addLast(page);
      } else if (borrowedPages.remove(page)) {
        releasePage(page, this);
      }
    }
  }

  private static class PageHandle implements MergeMemoryPage
  {
    private final Lease lease;

    private PageHandle(final Lease lease)
    {
      this.lease = lease;
    }

    @Override
    public ByteBuffer get()
    {
      return lease.get(this);
    }

    @Override
    public void close()
    {
      lease.release(this);
    }
  }

  private static class PhysicalPage
  {
    private final BackingAllocation backing;
    private final ByteBuffer buffer;
    @Nullable
    private Lease owner;

    private PhysicalPage(final BackingAllocation backing, final ByteBuffer buffer)
    {
      this.backing = backing;
      this.buffer = buffer;
    }
  }

  private static class BackingAllocation
  {
    private final ResourceHolder<ByteBuffer> holder;
    private final List<PhysicalPage> pages = new ArrayList<>();
    private int assignedPages;

    private BackingAllocation(final ResourceHolder<ByteBuffer> holder)
    {
      this.holder = holder;
    }
  }

  private static int divideRoundUp(final int dividend, final int divisor)
  {
    return dividend == 0 ? 0 : (dividend - 1) / divisor + 1;
  }

  private static QueryCapacityExceededException capacityExceeded()
  {
    return QueryCapacityExceededException.withErrorMessageAndResolvedHost(
        "Cannot acquire merge-memory pages. Try again after current running queries are finished."
    );
  }
}
