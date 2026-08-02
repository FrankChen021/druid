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
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryResults;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

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
    Assert.assertEquals(3, (int) QueryHandler.getSystemTasksMaxRows(queryResults.capture.bindableRel()));
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
    Assert.assertNull(QueryHandler.getSystemTasksMaxRows(queryResults.capture.bindableRel()));
  }

  @Test
  public void testGetSystemTasksMaxRowsWithExactScanFilter()
  {
    msqIncompatible();

    final QueryResults queryResults = testBuilder()
        .sql("SELECT task_id FROM sys.tasks WHERE type = 'testType' LIMIT 2")
        .queryContext(ImmutableMap.of(PlannerCaptureHook.NEED_CAPTURE_HOOK, true))
        .results();

    Assert.assertNull(queryResults.exception);
    Assert.assertEquals(2, (int) QueryHandler.getSystemTasksMaxRows(queryResults.capture.bindableRel()));
  }

  @Test
  public void testGetSystemTasksMaxRowsRejectsResidualFilter()
  {
    msqIncompatible();

    final QueryResults queryResults = testBuilder()
        .sql("SELECT task_id FROM sys.tasks LIMIT 2")
        .queryContext(ImmutableMap.of(PlannerCaptureHook.NEED_CAPTURE_HOOK, true))
        .results();

    Assert.assertNull(queryResults.exception);
    final Sort sort = getSort(queryResults.capture.bindableRel());
    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final RelNode input = sort.getInput();
    final RelNode residualFilter = Bindables.BindableFilter.create(
        input,
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(input, 0),
            rexBuilder.makeLiteral("task_id_1")
        )
    );
    final Sort sortWithResidualFilter = sort.copy(
        sort.getTraitSet(),
        residualFilter,
        sort.getCollation(),
        sort.offset,
        sort.fetch
    );

    Assert.assertNull(QueryHandler.getSystemTasksMaxRows(sortWithResidualFilter));
  }

  @Test
  public void testGetSystemTasksMaxRowsRejectsLimitAboveIntegerMax()
  {
    msqIncompatible();

    final QueryResults queryResults = testBuilder()
        .sql("SELECT task_id FROM sys.tasks LIMIT 2")
        .queryContext(ImmutableMap.of(PlannerCaptureHook.NEED_CAPTURE_HOOK, true))
        .results();

    Assert.assertNull(queryResults.exception);
    final Sort sort = getSort(queryResults.capture.bindableRel());
    final Sort sortWithLargeLimit = sort.copy(
        sort.getTraitSet(),
        sort.getInput(),
        sort.getCollation(),
        sort.offset,
        sort.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE))
    );

    Assert.assertNull(QueryHandler.getSystemTasksMaxRows(sortWithLargeLimit));
  }

  private static Sort getSort(final RelNode root)
  {
    RelNode current = root;
    while (current instanceof Project) {
      current = ((Project) current).getInput();
    }
    Assert.assertTrue(current instanceof Sort);
    return (Sort) current;
  }
}
