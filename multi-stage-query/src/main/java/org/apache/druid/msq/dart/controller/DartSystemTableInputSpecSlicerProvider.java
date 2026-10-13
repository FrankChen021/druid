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

package org.apache.druid.msq.dart.controller;

import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.InputSpecSlicerProvider;
import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/** Controller-side provider for Dart system-table inputs. */
public class DartSystemTableInputSpecSlicerProvider implements InputSpecSlicerProvider
{
  private final SystemTableNodeLocator nodeLocator;
  private final Map<String, SystemTableDescriptor> tableDescriptors;

  @Inject
  public DartSystemTableInputSpecSlicerProvider(
      final SystemTableNodeLocator nodeLocator,
      final Map<String, SystemTableDescriptor> tableDescriptors
  )
  {
    this.nodeLocator = nodeLocator;
    this.tableDescriptors = tableDescriptors;
  }

  @Override
  public Class<? extends InputSpec> specClass()
  {
    return SystemTableInputSpec.class;
  }

  @Override
  public InputSpecSlicer createSlicer(
      final ControllerContext controllerContext,
      final QueryContext queryContext,
      final List<String> workerIds
  )
  {
    return new DartSystemTableInputSpecSlicer(
        nodeLocator,
        tableDescriptors,
        workerIds,
        getQueryFailTime(queryContext)
    );
  }

  private static long getQueryFailTime(final QueryContext queryContext)
  {
    long queryFailTime = queryContext.getLong(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE);
    DateTime deadline = MultiStageQueryContext.getQueryDeadline(queryContext);
    if (deadline == null) {
      final long timeout = queryContext.getTimeout(QueryContexts.NO_TIMEOUT);
      if (timeout != QueryContexts.NO_TIMEOUT) {
        deadline = MultiStageQueryContext.getStartTime(queryContext).plus(timeout);
      }
    }
    if (deadline != null) {
      queryFailTime = Math.min(queryFailTime, deadline.getMillis());
    }
    return queryFailTime;
  }
}
