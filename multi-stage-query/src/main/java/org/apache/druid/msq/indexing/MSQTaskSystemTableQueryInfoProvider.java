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

package org.apache.druid.msq.indexing;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.apache.druid.server.system.table.SystemTableQueryInfoProvider;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Contributes active MSQ controller tasks from the local Overlord to the native {@code sys.queries} table. */
public class MSQTaskSystemTableQueryInfoProvider implements SystemTableQueryInfoProvider
{
  private static final String ACCEPTED_STATE = "ACCEPTED";
  private static final String RUNNING_STATE = "RUNNING";

  private final TaskMaster taskMaster;
  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;

  @Inject
  public MSQTaskSystemTableQueryInfoProvider(
      final TaskMaster taskMaster,
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles
  )
  {
    this.taskMaster = taskMaster;
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
  }

  @Override
  public Iterable<SystemTableQueryInfo> getQueryInfo()
  {
    final Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (!taskQueue.isPresent()) {
      return List.of();
    }
    final TaskRunner taskRunner = taskMaster.getTaskRunner().get();

    final List<SystemTableQueryInfo> queryInfo = new ArrayList<>();
    for (final Task task : taskQueue.get().getActiveTasks()) {
      if (!(task instanceof MSQControllerTask)) {
        continue;
      }
      final MSQControllerTask controllerTask = (MSQControllerTask) task;
      final String initialQueryId = controllerTask.getQuerySpec()
                                                  .getContext()
                                                  .getString(QueryContexts.CTX_SQL_QUERY_ID, controllerTask.getId());
      queryInfo.add(
          new SystemTableQueryInfo(
              controllerTask.getId(),
              MSQTaskSqlEngine.NAME,
              getState(taskRunner, controllerTask.getId()),
              makeInfo(controllerTask),
              initialQueryId,
              true,
              selfNode.getHostAndPortToUse(),
              selfNodeRoles.stream()
                           .map(NodeRole::getJsonName)
                           .sorted()
                           .collect(Collectors.joining(","))
          )
      );
    }
    return queryInfo;
  }

  private static String getState(final TaskRunner taskRunner, final String taskId)
  {
    return taskRunner.getRunnerTaskState(taskId) == RunnerTaskState.RUNNING ? RUNNING_STATE : ACCEPTED_STATE;
  }

  private static StructuredData makeInfo(final MSQControllerTask controllerTask)
  {
    final Map<String, Object> info = new LinkedHashMap<>();
    info.put("taskId", controllerTask.getId());
    info.put("queryType", controllerTask.getType());
    info.put("datasource", controllerTask.getDataSource());
    return StructuredData.wrap(info);
  }
}
