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

package org.apache.druid.server;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.server.system.table.StackTraceTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.Thread.State;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StackTraceTableDataProviderCollectionTest
{
  @Test
  public void testCollectsCurrentThreadWithoutThreadInfoTruncation()
  {
    final StackTraceTableDataProvider.ThreadStackTraceResponse response =
        StackTraceTableDataProvider.collect();
    Assertions.assertNotNull(response.getCollectedAt());
    Assertions.assertFalse(response.getThreads().isEmpty());

    final long currentThreadId = Thread.currentThread().threadId();
    final StackTraceTableDataProvider.ThreadStackTrace currentThread = response.getThreads()
                                                                       .stream()
                                                                       .filter(thread -> thread.getThreadId() == currentThreadId)
                                                                       .findFirst()
                                                                       .orElse(null);
    Assertions.assertNotNull(currentThread);
    Assertions.assertEquals(Thread.currentThread().getName(), currentThread.getThreadName());
    Assertions.assertEquals(State.RUNNABLE.name(), currentThread.getThreadState());
    Assertions.assertFalse(currentThread.getStackTrace().isEmpty());
    Assertions.assertTrue(currentThread.getStackTrace().contains("\n\tat "));
    Assertions.assertTrue(
        currentThread.getStackTrace().lines().filter(line -> line.startsWith("\tat ")).count() > 8
    );
    Assertions.assertFalse(currentThread.getStackTrace().contains("\t...\n"));
  }

  @Test
  public void testCollectUsesConfiguredMaxDepth()
  {
    final StackTraceTableDataProvider.ThreadStackTraceResponse response =
        StackTraceTableDataProvider.collect(10);
    Assertions.assertTrue(
        response.getThreads()
                .stream()
                .allMatch(
                    thread -> thread.getStackTrace()
                                   .lines()
                                   .filter(line -> line.startsWith("\tat "))
                                   .count() <= 10
                )
    );
  }

  @Test
  public void testQueryContextDepthConversionAndValidation()
  {
    Assertions.assertEquals(10, StackTraceTableDataProvider.getMaxStackTraceFrameDepth(10.9));
    Assertions.assertEquals(10, StackTraceTableDataProvider.getMaxStackTraceFrameDepth("10.00"));
    Assertions.assertEquals(
        StackTraceTableDataProvider.DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH,
        StackTraceTableDataProvider.getMaxStackTraceFrameDepth(null)
    );
    Assertions.assertThrows(
        DruidException.class,
        () -> StackTraceTableDataProvider.getMaxStackTraceFrameDepth(9)
    );
    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> StackTraceTableDataProvider.getMaxStackTraceFrameDepth("10.5")
    );
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

      final StackTraceTableDataProvider.ThreadStackTrace thread =
          StackTraceTableDataProvider.collect()
                                            .getThreads()
                                            .stream()
                                            .filter(stackTrace -> stackTrace.getThreadId() == waitingThread.threadId())
                                            .findFirst()
                                            .orElse(null);
      Assertions.assertNotNull(thread);
      Assertions.assertEquals(State.WAITING.name(), thread.getThreadState());
      Assertions.assertTrue(thread.getStackTrace().contains(" - waiting on " + thread.getLockName() + "\n"));
      Assertions.assertFalse(thread.getStackTrace().contains("\n\t-  waiting on "));
    }
    finally {
      waitingThread.interrupt();
      waitingThread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }
}
