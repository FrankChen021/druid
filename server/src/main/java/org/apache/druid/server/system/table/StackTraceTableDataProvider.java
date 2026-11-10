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
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
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

/**
 * Native row supplier for {@code sys.stack_trace}.
 *
 * <p>Each call takes a thread dump of this service, so the query must filter on {@code server} with {@code =} or
 * {@code IN}. The service returns no rows unless it matches the filters on {@code server} and {@code service_name}.</p>
 */
public class StackTraceTableDataProvider implements SystemTableDataProvider
{
  private static final String SERVER_COLUMN = "server";
  private static final String SERVICE_NAME_COLUMN = "service_name";
  static final String SERVER_FILTER_REQUIRED_MESSAGE =
      "sys.stack_trace requires a filter on the server column using '=' or 'IN'";

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
    if (!matchesAll(filters, SERVER_COLUMN, server)
        || !matchesAll(filters, SERVICE_NAME_COLUMN, selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final String nodeRoles = selfNodeRoles.stream()
                                          .map(NodeRole::getJsonName)
                                          .sorted()
                                          .toList()
                                          .toString();
    return collectRows(server, selfNode.getServiceName(), nodeRoles, maxStackTraceFrameDepth);
  }

  /**
   * Whether one of the filters requires {@code server} to equal one of a set of values.
   */
  static boolean hasServerFilter(final Iterable<Filter> filters)
  {
    for (final Filter filter : filters) {
      if (getStringValues(filter, SERVER_COLUMN) != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether every filter that requires the column to equal one of a set of values accepts the given value.
   */
  private static boolean matchesAll(final List<Filter> filters, final String column, final String value)
  {
    for (final Filter filter : filters) {
      final Set<String> values = getStringValues(filter, column);
      if (values != null && !values.contains(value)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the values one of which the filter requires the column to equal, or null if it isn't such a filter.
   * Supports {@code =}, {@code IN}, and an {@code OR} of them on the same column, comparing values as strings.
   */
  @Nullable
  static Set<String> getStringValues(final Filter filter, final String column)
  {
    if (filter instanceof SelectorFilter selector) {
      return column.equals(selector.getDimension()) && selector.getValue() != null
             ? Set.of(selector.getValue())
             : null;
    } else if (filter instanceof EqualityFilter equality) {
      // Other match types compare values as that type, such as "01" equal to "1" as LONG.
      return column.equals(equality.getColumn())
             && ColumnType.STRING.equals(equality.getMatchValueType())
             && equality.getMatchValue() instanceof String value
             ? Set.of(value)
             : null;
    } else if (filter instanceof InDimFilter in) {
      return column.equals(in.getDimension())
             && in.getExtractionFn() == null
             && !in.getValues().isEmpty()
             && !in.getValues().contains(null)
             ? Set.copyOf(in.getValues())
             : null;
    } else if (filter instanceof TypedInFilter in) {
      if (!column.equals(in.getColumn())
          || !ColumnType.STRING.equals(in.getMatchValueType())
          || in.getSortedValues().isEmpty()) {
        return null;
      }
      final Set<String> values = new HashSet<>();
      for (final Object value : in.getSortedValues()) {
        if (!(value instanceof String stringValue)) {
          return null;
        }
        values.add(stringValue);
      }
      return values;
    } else if (filter instanceof OrFilter or) {
      final Set<String> values = new HashSet<>();
      for (final Filter child : or.getFilters()) {
        final Set<String> childValues = getStringValues(child, column);
        if (childValues == null) {
          return null;
        }
        values.addAll(childValues);
      }
      return values.isEmpty() ? null : values;
    }
    return null;
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
   * Formats a thread stack in the format of {@link ThreadInfo#toString()}, but includes all frames returned by the
   * MX bean. {@code ThreadInfo.toString()} intentionally limits
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
