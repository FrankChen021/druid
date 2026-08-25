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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QueryMemoryManagerTest
{
  @Test
  public void testElasticAllocationRespectsNodeAndQueryBudgets()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 60, 1_000);
    final QueryMemoryAccount firstAccount = manager.openAccount("first");
    final QueryMemoryAccount secondAccount = manager.openAccount("second");

    final MemoryLease firstLease = firstAccount.tryAcquire(40, 40, MemoryPurpose.GROUP_BY_AGGREGATION).orElseThrow();
    Assertions.assertEquals(40, firstLease.size());

    final MemoryLease secondLease = firstAccount.tryAcquire(1, 40, MemoryPurpose.GROUP_BY_DICTIONARY).orElseThrow();
    Assertions.assertEquals(20, secondLease.size());
    Assertions.assertTrue(secondAccount.tryAcquire(41, 41, MemoryPurpose.OTHER).isEmpty());
    Assertions.assertEquals(60, firstAccount.currentBytes());
    Assertions.assertEquals(60, manager.getCurrentBytes());

    firstLease.close();
    secondLease.close();
    Assertions.assertEquals(0, manager.getCurrentBytes());
    firstAccount.close();
    secondAccount.close();
  }

  @Test
  public void testAccountCloseReleasesOutstandingLeasesIdempotently()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 1_000);
    final QueryMemoryAccount account = manager.openAccount("query");
    final MemoryLease lease = account.acquireMinimum(40, MemoryPurpose.SORT, 0);

    Assertions.assertEquals(40, manager.getCurrentBytes());
    account.close();
    account.close();
    lease.close();
    lease.close();

    Assertions.assertEquals(0, manager.getCurrentBytes());
    Assertions.assertEquals(0, manager.getActiveAccountCount());
    Assertions.assertTrue(account.isClosed());
  }

  @Test
  public void testMinimumAdmissionIsFifo()
      throws Exception
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 2_000);
    final QueryMemoryAccount holder = manager.openAccount("holder");
    final QueryMemoryAccount firstWaiter = manager.openAccount("first-waiter");
    final QueryMemoryAccount secondWaiter = manager.openAccount("second-waiter");
    final MemoryLease heldLease = holder.acquireMinimum(80, MemoryPurpose.GROUP_BY_MERGE, 0);
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      final Future<MemoryLease> firstFuture = executor.submit(
          () -> firstWaiter.acquireMinimum(80, MemoryPurpose.GROUP_BY_MERGE, 2_000)
      );
      final Future<MemoryLease> secondFuture = executor.submit(
          () -> secondWaiter.acquireMinimum(30, MemoryPurpose.GROUP_BY_MERGE, 2_000)
      );
      waitForPendingRequests(manager, 2);

      heldLease.close();
      final MemoryLease firstLease = firstFuture.get(2, TimeUnit.SECONDS);
      Assertions.assertFalse(secondFuture.isDone());

      firstLease.close();
      final MemoryLease secondLease = secondFuture.get(2, TimeUnit.SECONDS);
      secondLease.close();
    }
    finally {
      heldLease.close();
      executor.shutdownNow();
      holder.close();
      firstWaiter.close();
      secondWaiter.close();
    }
  }

  @Test
  public void testReclaimerRunsBeforeElasticRequestIsRetried()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 1_000);
    final QueryMemoryAccount account = manager.openAccount("query");
    final MemoryLease heldLease = account.acquireMinimum(80, MemoryPurpose.GROUP_BY_DICTIONARY, 0);
    final AtomicReference<MemoryLease> leaseReference = new AtomicReference<>(heldLease);
    final AtomicBoolean reclaimed = new AtomicBoolean();

    account.registerReclaimer(targetBytes -> {
      reclaimed.set(true);
      final MemoryLease lease = leaseReference.getAndSet(null);
      if (lease == null) {
        return 0;
      }
      final long releasedBytes = lease.size();
      lease.close();
      return releasedBytes;
    });

    final Optional<MemoryLease> acquired = account.tryAcquire(50, 50, MemoryPurpose.SORT);
    Assertions.assertTrue(reclaimed.get());
    Assertions.assertEquals(50, acquired.orElseThrow().size());
    Assertions.assertEquals(50, manager.getCurrentBytes());

    acquired.orElseThrow().close();
    account.close();
  }

  @Test
  public void testTimeoutAndCancellationProduceStructuredReasons()
      throws Exception
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 1_000);
    final QueryMemoryAccount holder = manager.openAccount("holder");
    final QueryMemoryAccount timedOut = manager.openAccount("timed-out");
    final QueryMemoryAccount canceled = manager.openAccount("canceled");
    final MemoryLease heldLease = holder.acquireMinimum(100, MemoryPurpose.OTHER, 0);

    final QueryMemoryException timeout = Assertions.assertThrows(
        QueryMemoryException.class,
        () -> timedOut.acquireMinimum(1, MemoryPurpose.OTHER, 20)
    );
    Assertions.assertEquals(QueryMemoryException.Reason.TIMEOUT, timeout.getReason());

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<MemoryLease> future = executor.submit(
          () -> canceled.acquireMinimum(1, MemoryPurpose.OTHER, 5_000)
      );
      waitForPendingRequests(manager, 1);
      canceled.cancel();

      final ExecutionException executionException = Assertions.assertThrows(
          ExecutionException.class,
          () -> future.get(2, TimeUnit.SECONDS)
      );
      Assertions.assertInstanceOf(QueryMemoryException.class, executionException.getCause());
      Assertions.assertEquals(
          QueryMemoryException.Reason.CANCELED,
          ((QueryMemoryException) executionException.getCause()).getReason()
      );
    }
    finally {
      executor.shutdownNow();
      heldLease.close();
      holder.close();
      timedOut.close();
      canceled.close();
    }
  }

  @Test
  public void testInterruptionRemovesPendingRequest()
      throws Exception
  {
    final QueryMemoryManager manager = new QueryMemoryManager(10, 10, 1_000);
    final QueryMemoryAccount holder = manager.openAccount("holder");
    final QueryMemoryAccount interrupted = manager.openAccount("interrupted");
    final MemoryLease heldLease = holder.acquireMinimum(10, MemoryPurpose.OTHER, 0);
    final AtomicReference<QueryMemoryException> failure = new AtomicReference<>();
    final Thread thread = new Thread(
        () -> {
          try {
            interrupted.acquireMinimum(1, MemoryPurpose.OTHER, 5_000);
          }
          catch (QueryMemoryException e) {
            failure.set(e);
          }
        }
    );
    thread.start();
    waitForPendingRequests(manager, 1);
    thread.interrupt();
    thread.join(2_000);

    Assertions.assertFalse(thread.isAlive());
    Assertions.assertNotNull(failure.get());
    Assertions.assertEquals(QueryMemoryException.Reason.INTERRUPTED, failure.get().getReason());
    Assertions.assertEquals(0, manager.getPendingRequestCount());

    heldLease.close();
    holder.close();
    interrupted.close();
  }

  @Test
  public void testInvalidRequestsAreStructured()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 1_000);
    final QueryMemoryAccount account = manager.openAccount("query");

    final QueryMemoryException invalid = Assertions.assertThrows(
        QueryMemoryException.class,
        () -> account.tryAcquire(20, 10, MemoryPurpose.OTHER)
    );
    Assertions.assertEquals(QueryMemoryException.Reason.INVALID_REQUEST, invalid.getReason());
    account.close();
  }

  @Test
  public void testFfmLeaseOwnsCloseableSharedSegment()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(
        1 << 20,
        1 << 20,
        1_000,
        new FfmNativeMemoryAllocator()
    );
    final QueryMemoryAccount account = manager.openAccount("ffm");
    final MemoryLease lease = account.acquireMinimum(4_096, MemoryPurpose.FRAME, 0);
    final MemorySegment segment = lease.segment();

    Assertions.assertEquals(4_096, segment.byteSize());
    Assertions.assertTrue(segment.scope().isAlive());
    lease.close();
    Assertions.assertFalse(segment.scope().isAlive());
    Assertions.assertEquals(0, manager.getCurrentBytes());
    account.close();
  }

  @Test
  public void testPhysicalAllocationFailureRollsBackAccounting()
  {
    final QueryMemoryManager manager = new QueryMemoryManager(
        100,
        100,
        1_000,
        bytes -> {
          throw new OutOfMemoryError("test allocation failure");
        }
    );
    final QueryMemoryAccount account = manager.openAccount("oom");

    final QueryMemoryException failure = Assertions.assertThrows(
        QueryMemoryException.class,
        () -> account.acquireMinimum(40, MemoryPurpose.OTHER, 0)
    );
    Assertions.assertEquals(QueryMemoryException.Reason.PHYSICAL_ALLOCATION_FAILED, failure.getReason());
    Assertions.assertEquals(0, manager.getCurrentBytes());
    Assertions.assertEquals(0, account.currentBytes());
    account.close();
  }

  @Test
  public void testCancellationDuringPhysicalAllocationReleasesReservation()
      throws Exception
  {
    final CountDownLatch allocationStarted = new CountDownLatch(1);
    final CountDownLatch allocationMayFinish = new CountDownLatch(1);
    final NativeMemoryAllocator blockingAllocator = bytes -> {
      allocationStarted.countDown();
      try {
        allocationMayFinish.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("allocation interrupted", e);
      }
      return new FfmNativeMemoryAllocator().allocate(bytes);
    };
    final QueryMemoryManager manager = new QueryMemoryManager(100, 100, 1_000, blockingAllocator);
    final QueryMemoryAccount account = manager.openAccount("canceled");
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    try {
      final Future<MemoryLease> future = executor.submit(
          () -> account.acquireMinimum(40, MemoryPurpose.OTHER, 5_000)
      );
      Assertions.assertTrue(allocationStarted.await(2, TimeUnit.SECONDS));
      account.cancel();
      allocationMayFinish.countDown();

      final ExecutionException executionException = Assertions.assertThrows(
          ExecutionException.class,
          () -> future.get(2, TimeUnit.SECONDS)
      );
      Assertions.assertInstanceOf(QueryMemoryException.class, executionException.getCause());
      Assertions.assertEquals(
          QueryMemoryException.Reason.CANCELED,
          ((QueryMemoryException) executionException.getCause()).getReason()
      );
      Assertions.assertEquals(0, manager.getCurrentBytes());
    }
    finally {
      allocationMayFinish.countDown();
      executor.shutdownNow();
      account.close();
    }
  }

  private static void waitForPendingRequests(final QueryMemoryManager manager, final int expected)
      throws InterruptedException
  {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (manager.getPendingRequestCount() < expected) {
      if (System.nanoTime() >= deadline) {
        Assertions.fail("Timed out waiting for pending query-memory requests");
      }
      Thread.sleep(5);
    }
  }
}
