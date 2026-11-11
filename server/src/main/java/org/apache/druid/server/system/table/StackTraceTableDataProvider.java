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

package org.apache.druid.server.system.table;

import com.google.inject.Inject;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongUnaryOperator;

/**
 * Native row supplier for {@code sys.stack_trace}.
 *
 * <p>Each call takes a thread dump of this service. The service returns no rows unless it matches the filters on
 * {@code server} and {@code service_name}. SQL queries must filter on {@code server}, which
 * {@link StackTraceTableDescriptor#validateNodeQuery} checks on the Broker.</p>
 */
public class StackTraceTableDataProvider implements SystemTableDataProvider
{
  static final String SERVER_COLUMN = "server";
  private static final String SERVICE_NAME_COLUMN = "service_name";

  private static final Set<String> PUSHDOWN_COLUMNS = Set.of(SERVER_COLUMN, SERVICE_NAME_COLUMN);

  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;

  @Inject
  public StackTraceTableDataProvider(
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles
  )
  {
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
  }

  @Override
  public Set<String> getPushdownColumns()
  {
    return PUSHDOWN_COLUMNS;
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters)
  {
    return getRows(filters, QueryContext.empty());
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters, final QueryContext queryContext)
  {
    // Read the depth first, so an invalid value fails the query even on a service that returns no rows.
    final int maxStackTraceFrameDepth = queryContext.get(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH);

    final String server = selfNode.getHostAndPortToUse();
    if (!SystemTableFilters.allowsValue(filters, SERVER_COLUMN, server)
        || !SystemTableFilters.allowsValue(filters, SERVICE_NAME_COLUMN, selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final String nodeRoles = selfNodeRoles.stream()
                                          .map(NodeRole::getJsonName)
                                          .sorted()
                                          .toList()
                                          .toString();
    return collectRows(server, selfNode.getServiceName(), nodeRoles, maxStackTraceFrameDepth);
  }

  private static List<Object[]> collectRows(
      final String server,
      final String serviceName,
      final String nodeRoles,
      final int maxStackTraceFrameDepth
  )
  {
    final String collectedAt = DateTimes.nowUtc().toString();
    final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    final boolean cpuTimeEnabled = isCpuTimeEnabled(threadMxBean);
    final Set<Long> deadlockedThreadIds = findDeadlockedThreadIds(threadMxBean);
    final long[] threadIds = threadMxBean.getAllThreadIds();
    final ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(
        threadIds,
        threadMxBean.isObjectMonitorUsageSupported(),
        threadMxBean.isSynchronizerUsageSupported(),
        maxStackTraceFrameDepth
    );
    final List<Object[]> rows = new ArrayList<>(threadInfos.length);

    for (final ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null) {
        continue;
      }

      final long threadId = threadInfo.getThreadId();
      final long rawLockOwnerId = threadInfo.getLockOwnerId();
      rows.add(
          new Object[]{
              server,
              serviceName,
              nodeRoles,
              collectedAt,
              threadId,
              threadInfo.getThreadName(),
              threadInfo.getThreadState().name(),
              threadInfo.isDaemon() ? 1L : 0L,
              (long) threadInfo.getPriority(),
              cpuTimeEnabled ? getThreadTime(threadMxBean::getThreadCpuTime, threadId) : null,
              cpuTimeEnabled ? getThreadTime(threadMxBean::getThreadUserTime, threadId) : null,
              threadInfo.getLockName(),
              rawLockOwnerId < 0 ? null : rawLockOwnerId,
              threadInfo.getLockOwnerName(),
              deadlockedThreadIds.contains(threadId) ? 1L : 0L,
              formatThreadInfo(threadInfo),
              null
          }
      );
    }

    return rows;
  }

  /**
   * Formats a thread stack in the format of {@link ThreadInfo#toString()}, but includes all frames returned by the
   * MX bean, up to {@code maxStackTraceFrameDepth}. {@code ThreadInfo.toString()} intentionally limits its output to
   * eight frames and appends an ellipsis.
   */
  private static String formatThreadInfo(final ThreadInfo threadInfo)
  {
    final StringBuilder builder = new StringBuilder();
    builder.append('"')
           .append(threadInfo.getThreadName())
           .append('"')
           .append(threadInfo.isDaemon() ? " daemon" : "")
           .append(" prio=")
           .append(threadInfo.getPriority())
           .append(" Id=")
           .append(threadInfo.getThreadId())
           .append(' ')
           .append(threadInfo.getThreadState());

    if (threadInfo.getLockName() != null) {
      builder.append(" on ").append(threadInfo.getLockName());
    }
    if (threadInfo.getLockOwnerName() != null) {
      builder.append(" owned by \"")
             .append(threadInfo.getLockOwnerName())
             .append("\" Id=")
             .append(threadInfo.getLockOwnerId());
    }
    if (threadInfo.isSuspended()) {
      builder.append(" (suspended)");
    }
    if (threadInfo.isInNative()) {
      builder.append(" (in native)");
    }
    builder.append('\n');

    final StackTraceElement[] stackTrace = threadInfo.getStackTrace();
    final MonitorInfo[] lockedMonitors = threadInfo.getLockedMonitors();
    for (int i = 0; i < stackTrace.length; i++) {
      builder.append("\tat ").append(stackTrace[i]).append('\n');

      if (i == 0) {
        final LockInfo lockInfo = threadInfo.getLockInfo();
        if (lockInfo != null) {
          switch (threadInfo.getThreadState()) {
            case BLOCKED:
              builder.append("\t-  blocked on ").append(lockInfo).append('\n');
              break;
            case WAITING:
            case TIMED_WAITING:
              builder.append("\t-  waiting on ").append(lockInfo).append('\n');
              break;
            default:
              break;
          }
        }
      }

      for (final MonitorInfo monitorInfo : lockedMonitors) {
        if (monitorInfo.getLockedStackDepth() == i) {
          builder.append("\t-  locked ").append(monitorInfo).append('\n');
        }
      }
    }

    final LockInfo[] lockedSynchronizers = threadInfo.getLockedSynchronizers();
    if (lockedSynchronizers.length > 0) {
      builder.append("\n\tNumber of locked synchronizers = ")
             .append(lockedSynchronizers.length)
             .append('\n');
      for (final LockInfo lockedSynchronizer : lockedSynchronizers) {
        builder.append("\t- ").append(lockedSynchronizer).append('\n');
      }
    }

    return builder.append('\n').toString();
  }

  private static boolean isCpuTimeEnabled(final ThreadMXBean threadMxBean)
  {
    try {
      return threadMxBean.isThreadCpuTimeSupported() && threadMxBean.isThreadCpuTimeEnabled();
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return false;
    }
  }

  /**
   * Reads a thread's time in nanoseconds with a {@link ThreadMXBean} method such as
   * {@link ThreadMXBean#getThreadCpuTime}, or returns null if the time is unavailable.
   */
  @Nullable
  private static Long getThreadTime(final LongUnaryOperator threadTimeReader, final long threadId)
  {
    try {
      final long time = threadTimeReader.applyAsLong(threadId);
      return time < 0 ? null : time;
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return null;
    }
  }

  private static Set<Long> findDeadlockedThreadIds(final ThreadMXBean threadMxBean)
  {
    try {
      final long[] threadIds = threadMxBean.findDeadlockedThreads();
      if (threadIds == null) {
        return Collections.emptySet();
      }

      final Set<Long> deadlockedThreadIds = new HashSet<>();
      for (final long threadId : threadIds) {
        deadlockedThreadIds.add(threadId);
      }
      return deadlockedThreadIds;
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return Collections.emptySet();
    }
  }
}
