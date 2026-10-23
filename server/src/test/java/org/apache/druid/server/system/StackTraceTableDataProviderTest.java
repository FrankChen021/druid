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
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.StackTraceTableDataProvider;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StackTraceTableDataProviderTest
{
  @Test
  public void testAdvertisesServerAndServiceNamePushdownFilters()
  {
    final StackTraceTableDataProvider provider = provider();

    Assertions.assertEquals(
        List.of(
            new SystemTablePushdownFilter("server", null),
            new SystemTablePushdownFilter("service_name", null)
        ),
        provider.getPushdownFilters()
    );
  }

  @Test
  public void testReturnsStackRowsWithNodeMetadata()
  {
    final StackTraceTableDataProvider provider = provider();
    final List<Object[]> rows = toRows(provider.getRows(Collections.emptyList()));

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
    final DimFilter wrongServer = new SelectorDimFilter("server", "other:8080", null);
    final DimFilter wrongService = new SelectorDimFilter("service_name", "broker", null);

    Assertions.assertTrue(toRows(provider.getRows(List.of(wrongServer))).isEmpty());
    Assertions.assertTrue(toRows(provider.getRows(List.of(wrongService))).isEmpty());
    Assertions.assertFalse(
        toRows(
            provider.getRows(
                List.of(new InDimFilter("server", List.of("other:8080", "localhost:8080"), null))
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
            Collections.emptyList(),
            QueryContext.of(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH, 10)
        )
    );

    Assertions.assertFalse(rows.isEmpty());
    Assertions.assertTrue(
        rows.stream().allMatch(row -> countStackFrames((String) row[15]) <= 10)
    );
  }

  @Test
  public void testCollectsCurrentThreadWithoutThreadInfoTruncation()
  {
    final Object[] currentThread = findThreadRow(
        toRows(provider().getRows(Collections.emptyList())),
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
  public void testQueryContextDepthConversionAndValidation()
  {
    Assertions.assertEquals(10, QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.parse(10.9));
    Assertions.assertEquals(10, QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.parse("10.00"));
    Assertions.assertEquals(100, QueryContext.empty().get(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH));
    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.parse(9)
    );
    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.parse(1001)
    );
    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH.parse("10.5")
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

      final Object[] thread = findThreadRow(
          toRows(provider().getRows(Collections.emptyList())),
          waitingThread.threadId()
      );
      Assertions.assertNotNull(thread);
      Assertions.assertEquals(State.WAITING.name(), thread[6]);
      final String stack = (String) thread[15];
      Assertions.assertTrue(stack.contains(" - waiting on " + thread[11] + "\n"));
      Assertions.assertFalse(stack.contains("\n\t-  waiting on "));
    }
    finally {
      waitingThread.interrupt();
      waitingThread.join(TimeUnit.SECONDS.toMillis(5));
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
