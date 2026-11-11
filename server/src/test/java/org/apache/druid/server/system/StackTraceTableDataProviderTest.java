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

package org.apache.druid.server.system;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.StackTraceTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.lang.Thread.State;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class StackTraceTableDataProviderTest
{
  private static final List<Filter> SELF_SERVER_FILTER = List.of(new SelectorFilter("server", "localhost:8080"));

  @Test
  public void testAdvertisesServerAndServiceNamePushdownColumns()
  {
    Assertions.assertEquals(Set.of("server", "service_name"), provider().getPushdownColumns());
  }

  /**
   * A native query reaches only the contacted service, so the provider returns its own threads without a
   * {@code server} filter. The filter is required only for SQL, before fan-out.
   */
  @Test
  public void testReturnsOwnRowsWithoutServerFilter()
  {
    final List<Object[]> rows = toRows(provider().getRows(List.of()));

    Assertions.assertFalse(rows.isEmpty());
    Assertions.assertEquals("localhost:8080", rows.get(0)[0]);
  }

  @Test
  public void testReturnsStackRowsWithNodeMetadata()
  {
    final StackTraceTableDataProvider provider = provider();
    final List<Object[]> rows = toRows(provider.getRows(SELF_SERVER_FILTER));

    Assertions.assertFalse(rows.isEmpty());
    final Object[] row = rows.get(0);
    Assertions.assertEquals("localhost:8080", row[0]);
    Assertions.assertEquals("coordinator", row[1]);
    Assertions.assertEquals("[coordinator, overlord]", row[2]);
    Assertions.assertNotNull(row[3]);
    Assertions.assertInstanceOf(Long.class, row[4]);
    Assertions.assertInstanceOf(String.class, row[5]);
    Assertions.assertInstanceOf(String.class, row[15]);
    Assertions.assertTrue(((String) row[15]).contains("\n\tat "));
    Assertions.assertNull(row[16]);
  }

  @Test
  public void testAppliesServerServiceNameAndInFilters()
  {
    final StackTraceTableDataProvider provider = provider();
    final Filter wrongServer = new SelectorFilter("server", "other:8080");
    final Filter wrongService = new EqualityFilter("service_name", ColumnType.STRING, "broker", null);

    Assertions.assertTrue(toRows(provider.getRows(List.of(wrongServer))).isEmpty());
    Assertions.assertTrue(
        toRows(provider.getRows(List.of(SELF_SERVER_FILTER.get(0), wrongService))).isEmpty()
    );
    Assertions.assertFalse(
        toRows(
            provider.getRows(
                List.of(new InDimFilter("server", List.of("other:8080", "localhost:8080"), null))
            )
        ).isEmpty()
    );
    Assertions.assertFalse(
        toRows(
            provider.getRows(
                List.of(
                    new TypedInFilter(
                        "server",
                        ColumnType.STRING,
                        null,
                        List.of("localhost:8080", "other:8080"),
                        null
                    )
                )
            )
        ).isEmpty()
    );
    Assertions.assertFalse(
        toRows(
            provider.getRows(
                List.of(
                    new OrFilter(
                        List.of(
                            new EqualityFilter("server", ColumnType.STRING, "other:8080", null),
                            new EqualityFilter("server", ColumnType.STRING, "localhost:8080", null)
                        )
                    ),
                    new SelectorFilter("service_name", "coordinator")
                )
            )
        ).isEmpty()
    );
  }

  @Test
  public void testUsesMaxStackTraceFrameDepthFromQueryContext()
  {
    final StackTraceTableDataProvider provider = provider();
    final List<Object[]> rows = toRows(
        provider.getRows(
            SELF_SERVER_FILTER,
            QueryContext.of(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH, 10)
        )
    );

    Assertions.assertFalse(rows.isEmpty());
    Assertions.assertTrue(
        rows.stream().allMatch(row -> countStackFrames((String) row[15]) <= 10)
    );
  }

  /**
   * An invalid depth fails the query even on a service that the filters exclude.
   */
  @Test
  public void testRejectsInvalidMaxStackTraceFrameDepth()
  {
    final BadQueryContextException e = Assertions.assertThrows(
        BadQueryContextException.class,
        () -> provider().getRows(
            List.of(new SelectorFilter("server", "other:8080")),
            QueryContext.of(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH, 1001)
        )
    );
    Assertions.assertTrue(e.getMessage().contains("maxStackTraceFrameDepth"), e.getMessage());
  }

  @Test
  public void testCollectsCurrentThreadWithoutThreadInfoTruncation()
  {
    final Object[] currentThread = findThreadRow(
        toRows(provider().getRows(SELF_SERVER_FILTER)),
        Thread.currentThread().threadId()
    );
    Assertions.assertNotNull(currentThread);
    Assertions.assertEquals(Thread.currentThread().getName(), currentThread[5]);
    Assertions.assertEquals(State.RUNNABLE.name(), currentThread[6]);
    final String stack = (String) currentThread[15];
    Assertions.assertTrue(stack.contains("\n\tat "));
    Assertions.assertTrue(countStackFrames(stack) > 8);
    Assertions.assertFalse(stack.contains("\t...\n"));
  }

  @Test
  public void testFormatsWaitingLockOnStackFrame() throws Exception
  {
    final Object monitor = new Object();
    final CountDownLatch enteredMonitor = new CountDownLatch(1);
    final Thread waitingThread = new Thread(
        () -> {
          synchronized (monitor) {
            enteredMonitor.countDown();
            try {
              monitor.wait();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        },
        "stack-trace-waiting-thread"
    );
    waitingThread.start();

    try {
      Assertions.assertTrue(enteredMonitor.await(5, TimeUnit.SECONDS));
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (waitingThread.getState() != State.WAITING && System.nanoTime() < deadline) {
        Thread.yield();
      }

      final Object[] thread = findThreadRow(
          toRows(provider().getRows(SELF_SERVER_FILTER)),
          waitingThread.threadId()
      );
      Assertions.assertNotNull(thread);
      Assertions.assertEquals(State.WAITING.name(), thread[6]);
      final String stack = (String) thread[15];
      // Like ThreadInfo.toString(), the lock is on its own line after the first frame.
      final String[] lines = stack.split("\n");
      Assertions.assertTrue(lines[1].startsWith("\tat "), stack);
      Assertions.assertEquals("\t-  waiting on " + thread[11], lines[2], stack);
    }
    finally {
      waitingThread.interrupt();
      waitingThread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  public void testFormatsLockedMonitorAfterOwningFrame() throws Exception
  {
    final Object monitor = new Object();
    final CountDownLatch enteredMonitor = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread lockingThread = new Thread(
        () -> {
          synchronized (monitor) {
            enteredMonitor.countDown();
            awaitQuietly(release);
          }
        },
        "stack-trace-locking-thread"
    );
    lockingThread.start();

    try {
      Assertions.assertTrue(enteredMonitor.await(5, TimeUnit.SECONDS));
      awaitState(lockingThread, State.WAITING);

      final Object[] thread = findThreadRow(
          toRows(provider().getRows(SELF_SERVER_FILTER)),
          lockingThread.threadId()
      );
      Assertions.assertNotNull(thread);
      final String stack = (String) thread[15];
      final List<String> lines = stack.lines().toList();
      final int lockedLine = lines.indexOf("\t-  locked " + lockString(monitor));
      // Like ThreadInfo.toString(), the locked monitor follows the frame that entered it.
      Assertions.assertTrue(lockedLine > 1, stack);
      Assertions.assertTrue(lines.get(lockedLine - 1).startsWith("\tat "), stack);
      Assertions.assertTrue(lines.get(lockedLine - 1).contains(StackTraceTableDataProviderTest.class.getName()), stack);
    }
    finally {
      release.countDown();
      lockingThread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  public void testFormatsBlockedLockOnStackFrame() throws Exception
  {
    final Object monitor = new Object();
    final Thread blockedThread = new Thread(
        () -> {
          synchronized (monitor) {
            // The test only observes the thread while it is blocked on entry.
            monitor.notifyAll();
          }
        },
        "stack-trace-blocked-thread"
    );

    final Object[] thread;
    synchronized (monitor) {
      blockedThread.start();
      awaitState(blockedThread, State.BLOCKED);
      thread = findThreadRow(toRows(provider().getRows(SELF_SERVER_FILTER)), blockedThread.threadId());
    }
    blockedThread.join(TimeUnit.SECONDS.toMillis(5));

    Assertions.assertNotNull(thread);
    Assertions.assertEquals(State.BLOCKED.name(), thread[6]);
    final String lock = lockString(monitor);
    Assertions.assertEquals(lock, thread[11]);
    final String stack = (String) thread[15];
    final String[] lines = stack.split("\n");
    Assertions.assertEquals(
        StringUtils.format(
            "\"stack-trace-blocked-thread\"%s prio=%d Id=%d BLOCKED on %s owned by \"%s\" Id=%d",
            blockedThread.isDaemon() ? " daemon" : "",
            blockedThread.getPriority(),
            blockedThread.threadId(),
            lock,
            Thread.currentThread().getName(),
            Thread.currentThread().threadId()
        ),
        lines[0],
        stack
    );
    Assertions.assertTrue(lines[1].startsWith("\tat "), stack);
    Assertions.assertEquals("\t-  blocked on " + lock, lines[2], stack);
  }

  @Test
  public void testFormatsLockedSynchronizers() throws Exception
  {
    final ReentrantLock lock = new ReentrantLock();
    final CountDownLatch acquiredLock = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread lockingThread = new Thread(
        () -> {
          lock.lock();
          try {
            acquiredLock.countDown();
            awaitQuietly(release);
          }
          finally {
            lock.unlock();
          }
        },
        "stack-trace-synchronizer-thread"
    );
    lockingThread.start();

    try {
      Assertions.assertTrue(acquiredLock.await(5, TimeUnit.SECONDS));
      awaitState(lockingThread, State.WAITING);

      final ThreadInfo threadInfo =
          ManagementFactory.getThreadMXBean().getThreadInfo(new long[]{lockingThread.threadId()}, true, true)[0];
      Assertions.assertNotNull(threadInfo);
      Assertions.assertEquals(1, threadInfo.getLockedSynchronizers().length);

      final Object[] thread = findThreadRow(
          toRows(provider().getRows(SELF_SERVER_FILTER)),
          lockingThread.threadId()
      );
      Assertions.assertNotNull(thread);
      final String stack = (String) thread[15];
      // Like ThreadInfo.toString(), the synchronizers follow the frames after a blank line, and the stack ends with
      // a blank line.
      Assertions.assertTrue(
          stack.endsWith(
              "\n\n\tNumber of locked synchronizers = 1\n\t- " + threadInfo.getLockedSynchronizers()[0] + "\n\n"
          ),
          stack
      );
    }
    finally {
      release.countDown();
      lockingThread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  /**
   * JVM-internal threads such as {@code Signal Dispatcher} have no Java frames. Like {@link ThreadInfo#toString()},
   * their stack is the header line followed by a blank line.
   */
  @Test
  public void testFormatsThreadWithoutFrames()
  {
    final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    final List<Long> threadIdsWithoutFrames = new ArrayList<>();
    for (final ThreadInfo threadInfo : threadMxBean.getThreadInfo(threadMxBean.getAllThreadIds(), true, true)) {
      if (threadInfo != null && threadInfo.getStackTrace().length == 0
          && threadInfo.getLockName() == null && threadInfo.getLockedSynchronizers().length == 0) {
        threadIdsWithoutFrames.add(threadInfo.getThreadId());
      }
    }
    Assumptions.assumeFalse(threadIdsWithoutFrames.isEmpty(), "No thread without frames in this JVM");

    final List<Object[]> rows = toRows(provider().getRows(SELF_SERVER_FILTER));
    boolean found = false;
    for (final long threadId : threadIdsWithoutFrames) {
      final Object[] thread = findThreadRow(rows, threadId);
      if (thread == null) {
        continue;
      }
      found = true;
      final String stack = (String) thread[15];
      Assertions.assertEquals(0, countStackFrames(stack), stack);
      Assertions.assertTrue(stack.endsWith("\n\n"), stack);
      final List<String> lines = stack.lines().toList();
      Assertions.assertEquals(List.of(lines.get(0), ""), lines, stack);
      // The header may end with " (suspended)" or " (in native)", as in ThreadInfo.toString().
      Assertions.assertTrue(
          lines.get(0).startsWith(
              StringUtils.format(
                  "\"%s\"%s prio=%d Id=%d %s",
                  thread[5],
                  Long.valueOf(1L).equals(thread[7]) ? " daemon" : "",
                  thread[8],
                  threadId,
                  thread[6]
              )
          ),
          stack
      );
    }
    Assertions.assertTrue(found);
  }

  private static String lockString(final Object lock)
  {
    return lock.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(lock));
  }

  private static void awaitState(final Thread thread, final State state)
  {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.getState() != state && System.nanoTime() < deadline) {
      Thread.yield();
    }
    Assertions.assertEquals(state, thread.getState());
  }

  private static void awaitQuietly(final CountDownLatch latch)
  {
    try {
      latch.await();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static StackTraceTableDataProvider provider()
  {
    return new StackTraceTableDataProvider(
        new DruidNode("coordinator", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.COORDINATOR, NodeRole.OVERLORD)
    );
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }

  @Nullable
  private static Object[] findThreadRow(final List<Object[]> rows, final long threadId)
  {
    return rows.stream().filter(row -> Long.valueOf(threadId).equals(row[4])).findFirst().orElse(null);
  }

  private static long countStackFrames(final String stackTrace)
  {
    return stackTrace.lines().filter(line -> line.startsWith("\tat ")).count();
  }
}
