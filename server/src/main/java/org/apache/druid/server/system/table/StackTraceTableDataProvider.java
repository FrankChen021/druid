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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
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

/** Native row supplier for {@code sys.stack_trace}. */
public class StackTraceTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", null),
      new SystemTablePushdownFilter("service_name", null)
  );

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
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters)
  {
    return getRows(filters, QueryContext.empty());
  }

  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters, final QueryContext queryContext)
  {
    final String server = selfNode.getHostAndPortToUse();
    if (!matchesNode(filters, "server", server)
        || !matchesNode(filters, "service_name", selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final int maxStackTraceFrameDepth = queryContext.get(QueryContextParameters.MAX_STACK_TRACE_FRAME_DEPTH);
    final String nodeRoles = selfNodeRoles.stream()
                                          .map(NodeRole::getJsonName)
                                          .sorted()
                                          .toList()
                                          .toString();
    return collectRows(server, selfNode.getServiceName(), nodeRoles, maxStackTraceFrameDepth);
  }

  private static boolean matchesNode(
      final List<DimFilter> filters,
      final String column,
      final String value
  )
  {
    return filters.stream()
                  .filter(StackTraceTableDataProvider::isStringValuesFilter)
                  .filter(filter -> column.equals(SystemTablePushdownFilter.getStringValuesColumn(filter)))
                  .allMatch(filter -> SystemTablePushdownFilter.getStringValues(filter).contains(value));
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
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
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, false),
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, true),
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
   * Formats a thread stack in a jstack-style format based on {@link ThreadInfo#toString()}, but
   * includes all frames returned by the MX bean. {@code ThreadInfo.toString()} intentionally limits
   * its output to eight frames and appends an ellipsis.
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
      builder.append("\tat ").append(stackTrace[i]);

      if (i == 0) {
        final LockInfo lockInfo = threadInfo.getLockInfo();
        if (lockInfo != null) {
          switch (threadInfo.getThreadState()) {
            case BLOCKED:
              builder.append(" - blocked on ").append(lockInfo);
              break;
            case WAITING:
            case TIMED_WAITING:
              builder.append(" - waiting on ").append(lockInfo);
              break;
            default:
              break;
          }
        }
      }

      builder.append('\n');

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

  @Nullable
  private static Long getThreadCpuTime(
      final ThreadMXBean threadMxBean,
      final long threadId,
      final boolean cpuTimeEnabled,
      final boolean userTime
  )
  {
    if (!cpuTimeEnabled) {
      return null;
    }

    try {
      final long cpuTime = userTime
                           ? threadMxBean.getThreadUserTime(threadId)
                           : threadMxBean.getThreadCpuTime(threadId);
      return cpuTime < 0 ? null : cpuTime;
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
