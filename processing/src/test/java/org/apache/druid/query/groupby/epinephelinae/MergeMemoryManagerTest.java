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

import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryResourceId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeMemoryManagerTest
{
  @Test
  public void testQueriesShareBackingBufferAndReturnItAfterLastLeaseCloses()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(2);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 4, 0);
    final MergeMemoryLease second = manager.acquireMinimum(new QueryResourceId("second"), 1, 4, 0);

    assertEquals(1, manager.getBackingAllocationCount());
    assertEquals(1, pool.getUsedResourcesCount());
    first.close();
    assertEquals(1, pool.getUsedResourcesCount());
    second.close();
    assertEquals(0, manager.getBackingAllocationCount());
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testGrowthAcquiresAnotherBackingWithoutBlocking()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(2);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease lease = manager.acquireMinimum(new QueryResourceId("query"), 1, 8, 0);
    final List<MergeMemoryPage> pages = lease.tryAcquirePages(5).orElseThrow();

    assertEquals(2, manager.getBackingAllocationCount());
    pages.forEach(MergeMemoryPage::close);
    assertEquals(1, manager.getBackingAllocationCount());
    lease.close();
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testQueryMaximumDeniesWholeRequest()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(2);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease lease = manager.acquireMinimum(new QueryResourceId("query"), 1, 2, 0);

    assertFalse(lease.tryAcquirePages(3).isPresent());
    assertEquals(1, manager.getBackingAllocationCount());
    lease.close();
  }

  @Test
  public void testGrowthDenialDoesNotConsumeFreePages()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 8, 0);
    final MergeMemoryLease second = manager.acquireMinimum(new QueryResourceId("second"), 3, 3, 0);

    assertFalse(first.tryAcquirePages(4).isPresent());
    assertEquals(0, manager.getFreePageCount());
    second.close();
    assertTrue(first.tryAcquirePages(3).isPresent());
    first.close();
  }

  @Test
  public void testPageHandleInvalidAfterLeaseClose()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease lease = manager.acquireMinimum(new QueryResourceId("query"), 1, 1, 0);
    final MergeMemoryPage page = lease.tryAcquirePages(1).orElseThrow().get(0);
    lease.close();

    assertThrows(ISE.class, page::get);
    page.close();
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testLaneMinimumCannotBeConsumedByAnotherLane()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final MergeMemoryManager manager = manager(pool);
    final MergeMemoryLease queryLease = manager.acquireMinimum(new QueryResourceId("query"), 4, 4, 0);
    final MergeMemoryLease firstLane = new LaneMergeMemoryLease(queryLease, 2);
    final MergeMemoryLease secondLane = new LaneMergeMemoryLease(queryLease, 2);

    assertFalse(firstLane.tryAcquirePages(3).isPresent());
    assertEquals(2, firstLane.tryAcquirePages(2).orElseThrow().size());
    assertEquals(2, secondLane.tryAcquirePages(2).orElseThrow().size());

    firstLane.close();
    secondLane.close();
    queryLease.close();
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testPendingMinimumAdmissionTakesPrecedenceOverGrowth() throws Exception
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(2);
    final BlockingPoolMergeMemoryBackingAllocator delegate = new BlockingPoolMergeMemoryBackingAllocator(pool, 64);
    final CountDownLatch admissionWaiting = new CountDownLatch(1);
    final CountDownLatch allowAdmission = new CountDownLatch(1);
    final MergeMemoryBackingAllocator allocator = new MergeMemoryBackingAllocator()
    {
      private int acquisitions;

      @Override
      public int allocationSize()
      {
        return delegate.allocationSize();
      }

      @Override
      public synchronized Optional<List<ResourceHolder<ByteBuffer>>> acquire(
          final int count,
          final long timeoutMillis
      )
      {
        if (acquisitions++ > 0) {
          admissionWaiting.countDown();
          try {
            allowAdmission.await();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return delegate.acquire(count, timeoutMillis);
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
      {
        return delegate.tryAcquire(count);
      }
    };
    final MergeMemoryManager manager = new MergeMemoryManager(allocator, 16);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 8, 0);
    final ExecutorService executor = Execs.singleThreaded("merge-memory-admission-test-%d");
    try {
      final Future<MergeMemoryLease> secondFuture = executor.submit(
          () -> manager.acquireMinimum(new QueryResourceId("second"), 4, 4, 0)
      );
      assertTrue(admissionWaiting.await(5, TimeUnit.SECONDS));
      assertFalse(first.tryAcquirePages(2).isPresent());
      allowAdmission.countDown();
      secondFuture.get(5, TimeUnit.SECONDS).close();
    }
    finally {
      allowAdmission.countDown();
      executor.shutdownNow();
      first.close();
    }
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testFailedBackingAllocationReturnsReservedPages()
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final BlockingPoolMergeMemoryBackingAllocator delegate = new BlockingPoolMergeMemoryBackingAllocator(pool, 64);
    final AtomicInteger acquisitions = new AtomicInteger();
    final MergeMemoryBackingAllocator allocator = new MergeMemoryBackingAllocator()
    {
      @Override
      public int allocationSize()
      {
        return delegate.allocationSize();
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> acquire(final int count, final long timeoutMillis)
      {
        if (acquisitions.getAndIncrement() > 0) {
          throw new ISE("allocation failed");
        }
        return delegate.acquire(count, timeoutMillis);
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
      {
        return delegate.tryAcquire(count);
      }
    };
    final MergeMemoryManager manager = new MergeMemoryManager(allocator, 16);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 4, 0);

    assertThrows(
        ISE.class,
        () -> manager.acquireMinimum(new QueryResourceId("second"), 4, 4, 0)
    );
    assertEquals(3, manager.getFreePageCount());
    first.tryAcquirePages(4).orElseThrow().forEach(MergeMemoryPage::close);
    first.close();
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testWaitingAdmissionDoesNotPinBackingNeededForProgress() throws Exception
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final BlockingPoolMergeMemoryBackingAllocator delegate = new BlockingPoolMergeMemoryBackingAllocator(pool, 64);
    final CountDownLatch waitingForBacking = new CountDownLatch(1);
    final AtomicInteger acquisitions = new AtomicInteger();
    final MergeMemoryBackingAllocator allocator = new MergeMemoryBackingAllocator()
    {
      @Override
      public int allocationSize()
      {
        return delegate.allocationSize();
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> acquire(final int count, final long timeoutMillis)
      {
        if (acquisitions.getAndIncrement() > 0) {
          waitingForBacking.countDown();
        }
        return delegate.acquire(count, timeoutMillis);
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
      {
        return delegate.tryAcquire(count);
      }
    };
    final MergeMemoryManager manager = new MergeMemoryManager(allocator, 16);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 4, 0);
    final ExecutorService executor = Execs.singleThreaded("merge-memory-deadlock-test-%d");
    final Future<MergeMemoryLease> secondFuture = executor.submit(
        () -> manager.acquireMinimum(new QueryResourceId("second"), 4, 4, -1)
    );
    try {
      assertTrue(waitingForBacking.await(5, TimeUnit.SECONDS));
      first.close();
      secondFuture.get(5, TimeUnit.SECONDS).close();
    }
    finally {
      secondFuture.cancel(true);
      executor.shutdownNow();
      first.close();
    }
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testAdmissionLockWaitCountsAgainstTimeout() throws Exception
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(1);
    final BlockingPoolMergeMemoryBackingAllocator delegate = new BlockingPoolMergeMemoryBackingAllocator(pool, 64);
    final CountDownLatch waitingForBacking = new CountDownLatch(1);
    final AtomicInteger acquisitions = new AtomicInteger();
    final MergeMemoryBackingAllocator allocator = new MergeMemoryBackingAllocator()
    {
      @Override
      public int allocationSize()
      {
        return delegate.allocationSize();
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> acquire(final int count, final long timeoutMillis)
      {
        if (acquisitions.getAndIncrement() > 0) {
          waitingForBacking.countDown();
        }
        return delegate.acquire(count, timeoutMillis);
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
      {
        return delegate.tryAcquire(count);
      }
    };
    final MergeMemoryManager manager = new MergeMemoryManager(allocator, 16);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 4, 0);
    final ExecutorService executor = Execs.singleThreaded("merge-memory-timeout-test-%d");
    final Future<MergeMemoryLease> untimedFuture = executor.submit(
        () -> manager.acquireMinimum(new QueryResourceId("untimed"), 4, 4, -1)
    );
    try {
      assertTrue(waitingForBacking.await(5, TimeUnit.SECONDS));
      assertThrows(
          QueryCapacityExceededException.class,
          () -> manager.acquireMinimum(new QueryResourceId("timed"), 1, 1, 10)
      );
      first.close();
      untimedFuture.get(5, TimeUnit.SECONDS).close();
    }
    finally {
      untimedFuture.cancel(true);
      executor.shutdownNow();
      first.close();
    }
    assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testAdmissionRechecksPagesAfterMultipleBackingsAreReleased() throws Exception
  {
    final DefaultBlockingPool<ByteBuffer> pool = pool(2);
    final BlockingPoolMergeMemoryBackingAllocator delegate = new BlockingPoolMergeMemoryBackingAllocator(pool, 64);
    final CountDownLatch allocationStarted = new CountDownLatch(1);
    final CountDownLatch allowAllocation = new CountDownLatch(1);
    final AtomicInteger acquisitions = new AtomicInteger();
    final MergeMemoryBackingAllocator allocator = new MergeMemoryBackingAllocator()
    {
      @Override
      public int allocationSize()
      {
        return delegate.allocationSize();
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> acquire(final int count, final long timeoutMillis)
      {
        if (acquisitions.getAndIncrement() == 2) {
          allocationStarted.countDown();
          try {
            allowAllocation.await();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return delegate.acquire(count, timeoutMillis);
      }

      @Override
      public Optional<List<ResourceHolder<ByteBuffer>>> tryAcquire(final int count)
      {
        return delegate.tryAcquire(count);
      }
    };
    final MergeMemoryManager manager = new MergeMemoryManager(allocator, 16);
    final MergeMemoryLease first = manager.acquireMinimum(new QueryResourceId("first"), 1, 4, 0);
    final List<MergeMemoryPage> firstPages = first.tryAcquirePages(4).orElseThrow();
    final MergeMemoryLease second = manager.acquireMinimum(new QueryResourceId("second"), 1, 1, 0);
    firstPages.get(1).close();

    final ExecutorService executor = Execs.singleThreaded("merge-memory-recheck-test-%d");
    final Future<MergeMemoryLease> waitingFuture = executor.submit(
        () -> manager.acquireMinimum(new QueryResourceId("waiting"), 5, 5, -1)
    );
    try {
      assertTrue(allocationStarted.await(5, TimeUnit.SECONDS));
      first.close();
      second.close();
      allowAllocation.countDown();
      waitingFuture.get(5, TimeUnit.SECONDS).close();
    }
    finally {
      allowAllocation.countDown();
      waitingFuture.cancel(true);
      executor.shutdownNow();
      first.close();
      second.close();
    }
    assertEquals(0, pool.getUsedResourcesCount());
  }

  private static DefaultBlockingPool<ByteBuffer> pool(final int buffers)
  {
    return new DefaultBlockingPool<>(() -> ByteBuffer.allocateDirect(64), buffers);
  }

  private static MergeMemoryManager manager(final DefaultBlockingPool<ByteBuffer> pool)
  {
    return new MergeMemoryManager(new BlockingPoolMergeMemoryBackingAllocator(pool, 64), 16);
  }
}
