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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Native row supplier for {@code sys.stack_trace}. */
public class SystemStackTraceTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", null),
      new SystemTablePushdownFilter("service_name", null)
  );

  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SystemStackTraceTableDataProvider(
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    return getRows(filters, internalAuthenticationResult, Collections.emptyMap());
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult,
      final Map<String, Object> queryContext
  )
  {
    authorizeStackTraceRead(internalAuthenticationResult);

    final String server = selfNode.getHostAndPortToUse();
    if (!matchesNode(filters, "server", server)
        || !matchesNode(filters, "service_name", selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final int maxStackTraceFrameDepth = getMaxStackTraceFrameDepth(
        queryContext.get(MAX_STACK_TRACE_FRAME_DEPTH_KEY)
    );
    final ThreadStackTraceResponse response = collect(maxStackTraceFrameDepth);
    final String nodeRoles = selfNodeRoles.stream()
                                          .map(NodeRole::getJsonName)
                                          .sorted()
                                          .collect(Collectors.joining(","));

    return response.getThreads()
                   .stream()
                   .map(thread -> new Object[]{
                       server,
                       selfNode.getServiceName(),
                       nodeRoles,
                       response.getCollectedAt(),
                       thread.getThreadId(),
                       thread.getThreadName(),
                       thread.getThreadState(),
                       thread.isDaemon() ? 1L : 0L,
                       (long) thread.getPriority(),
                       thread.getCpuTimeNs(),
                       thread.getUserCpuTimeNs(),
                       thread.getLockName(),
                       thread.getLockOwnerId(),
                       thread.getLockOwnerName(),
                       thread.isDeadlocked() ? 1L : 0L,
                       thread.getStackTrace(),
                       null
                   })
                   .collect(Collectors.toList());
  }

  private void authorizeStackTraceRead(final AuthenticationResult authenticationResult)
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(
          "Insufficient permission to view stack traces: " + authorizationResult.getErrorMessage()
      );
    }
  }

  private static boolean matchesNode(
      final List<DimFilter> filters,
      final String column,
      final String value
  )
  {
    return filters.stream()
                  .filter(SystemStackTraceTableDataProvider::isStringValuesFilter)
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

  public static final String MAX_STACK_TRACE_FRAME_DEPTH_KEY = "maxStackTraceFrameDepth";
  public static final int MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH = 10;
  public static final int DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH = 100;
  public static final int MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH = 1000;

  public static ThreadStackTraceResponse collect()
  {
    return collect(DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH);
  }

  public static ThreadStackTraceResponse collect(final int maxStackTraceFrameDepth)
  {
    validateMaxStackTraceFrameDepth(maxStackTraceFrameDepth);
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
    final List<ThreadStackTrace> threads = new ArrayList<>(threadInfos.length);

    for (final ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null) {
        continue;
      }

      final long threadId = threadInfo.getThreadId();
      final long rawLockOwnerId = threadInfo.getLockOwnerId();
      final Long lockOwnerId = rawLockOwnerId < 0 ? null : rawLockOwnerId;
      threads.add(
          new ThreadStackTrace(
              threadId,
              threadInfo.getThreadName(),
              threadInfo.getThreadState().name(),
              threadInfo.isDaemon(),
              threadInfo.getPriority(),
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, false),
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, true),
              threadInfo.getLockName(),
              lockOwnerId,
              threadInfo.getLockOwnerName(),
              deadlockedThreadIds.contains(threadId),
              formatThreadInfo(threadInfo)
          )
      );
    }

    return new ThreadStackTraceResponse(collectedAt, threads);
  }

  /**
   * Reads the maximum frame depth from a query context value. Query context conversion follows the standard Druid
   * rules: numeric values use {@link Number#longValue()}, while string values are parsed as longs (with support for
   * integral decimal strings).
   */
  public static int getMaxStackTraceFrameDepth(@Nullable final Object value)
  {
    return validateMaxStackTraceFrameDepth(
        QueryContexts.getAsLong(
            MAX_STACK_TRACE_FRAME_DEPTH_KEY,
            value,
            DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH
        )
    );
  }

  public static int validateMaxStackTraceFrameDepth(final long maxStackTraceFrameDepth)
  {
    InvalidInput.conditionalException(
        maxStackTraceFrameDepth >= MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        "[%s] must be greater than or equal to %d, but got[%d]",
        MAX_STACK_TRACE_FRAME_DEPTH_KEY,
        MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        maxStackTraceFrameDepth
    );
    InvalidInput.conditionalException(
        maxStackTraceFrameDepth <= MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        "[%s] must be less than or equal to %d, but got[%d]",
        MAX_STACK_TRACE_FRAME_DEPTH_KEY,
        MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        maxStackTraceFrameDepth
    );
    return Math.toIntExact(maxStackTraceFrameDepth);
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

  public static class ThreadStackTraceResponse
  {
    private final String collectedAt;
    private final List<ThreadStackTrace> threads;

    public ThreadStackTraceResponse(
        final String collectedAt,
        final List<ThreadStackTrace> threads
    )
    {
      this.collectedAt = collectedAt;
      this.threads = threads == null ? ImmutableList.of() : ImmutableList.copyOf(threads);
    }

    public String getCollectedAt()
    {
      return collectedAt;
    }

    public List<ThreadStackTrace> getThreads()
    {
      return threads;
    }
  }

  public static class ThreadStackTrace
  {
    private final long threadId;
    private final String threadName;
    private final String threadState;
    private final boolean daemon;
    private final int priority;
    @Nullable
    private final Long cpuTimeNs;
    @Nullable
    private final Long userCpuTimeNs;
    @Nullable
    private final String lockName;
    @Nullable
    private final Long lockOwnerId;
    @Nullable
    private final String lockOwnerName;
    private final boolean deadlocked;
    private final String stackTrace;

    public ThreadStackTrace(
        final long threadId,
        final String threadName,
        final String threadState,
        final boolean daemon,
        final int priority,
        @Nullable final Long cpuTimeNs,
        @Nullable final Long userCpuTimeNs,
        @Nullable final String lockName,
        @Nullable final Long lockOwnerId,
        @Nullable final String lockOwnerName,
        final boolean deadlocked,
        final String stackTrace
    )
    {
      this.threadId = threadId;
      this.threadName = threadName;
      this.threadState = threadState;
      this.daemon = daemon;
      this.priority = priority;
      this.cpuTimeNs = cpuTimeNs;
      this.userCpuTimeNs = userCpuTimeNs;
      this.lockName = lockName;
      this.lockOwnerId = lockOwnerId;
      this.lockOwnerName = lockOwnerName;
      this.deadlocked = deadlocked;
      this.stackTrace = stackTrace;
    }

    public long getThreadId()
    {
      return threadId;
    }

    public String getThreadName()
    {
      return threadName;
    }

    public String getThreadState()
    {
      return threadState;
    }

    public boolean isDaemon()
    {
      return daemon;
    }

    public int getPriority()
    {
      return priority;
    }

    @Nullable
    public Long getCpuTimeNs()
    {
      return cpuTimeNs;
    }

    @Nullable
    public Long getUserCpuTimeNs()
    {
      return userCpuTimeNs;
    }

    @Nullable
    public String getLockName()
    {
      return lockName;
    }

    @Nullable
    public Long getLockOwnerId()
    {
      return lockOwnerId;
    }

    @Nullable
    public String getLockOwnerName()
    {
      return lockOwnerName;
    }

    public boolean isDeadlocked()
    {
      return deadlocked;
    }

    public String getStackTrace()
    {
      return stackTrace;
    }
  }
}
