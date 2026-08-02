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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryResults;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class QueryHandlerTest extends BaseCalciteQueryTest
{
  @Test
  public void testGetSystemTasksMaxRows()
  {
    msqIncompatible();

    final QueryResults queryResults = testBuilder()
        .sql("SELECT task_id FROM sys.tasks LIMIT 2 OFFSET 1")
        .queryContext(ImmutableMap.of(PlannerCaptureHook.NEED_CAPTURE_HOOK, true))
        .results();

    Assert.assertNull(queryResults.exception);
    Assert.assertEquals(3, (int) QueryHandler.getSystemTasksMaxRows(queryResults.capture.relRoot().rel));
  }

  @Test
  public void testGetSystemTasksMaxRowsRejectsOrderedQuery()
  {
    msqIncompatible();

    final QueryResults queryResults = testBuilder()
        .sql("SELECT task_id FROM sys.tasks ORDER BY task_id LIMIT 2")
        .queryContext(ImmutableMap.of(PlannerCaptureHook.NEED_CAPTURE_HOOK, true))
        .results();

    Assert.assertNull(queryResults.exception);
    Assert.assertNull(QueryHandler.getSystemTasksMaxRows(queryResults.capture.relRoot().rel));
  }
}
