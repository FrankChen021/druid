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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

public class CalciteSysQueryTest extends BaseCalciteQueryTest
{
  @RegisterExtension
  NotYetSupportedProcessor notYetSupportedProcessor = new NotYetSupportedProcessor(NotYetSupported.Scope.BINDABLE);

  private static final String STACK_TRACE_REQUIRES_DRUID_ENGINE_MESSAGE =
      "sys.stack_trace can only be queried with the Druid engine: set the query context parameter "
      + "useDruidEngineForSystemTables to true and use the native SQL engine. The Druid engine must also be enabled "
      + "with druid.query.enableDruidEngineForSystemTables on all services.";

  @Test
  public void testTasksSum()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        .expectedLogicalPlan("LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
                             + "  LogicalProject(exprs=[[$3, $8]])\n"
                             + "    LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  @NotYetSupported(Modes.EXPRESSION_NOT_GROUPED)
  @Test
  public void testTasksSumOver()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) over () from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        // please add expectedLogicalPlan if this test starts passing!
        .run();
  }

  @Test
  public void testRoundOnSysTableColumn()
  {
    msqIncompatible();

    testBuilder()
        .sql("select round(duration, 1) from sys.tasks ")
        .expectedResults(ImmutableList.of(
            new Object[]{10L},
            new Object[]{1L},
            new Object[]{20L},
            new Object[]{2L}
        ))
        .expectedLogicalPlan("LogicalProject(exprs=[[ROUND($8, 1)]])\n"
                             + "  LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  @Test
  public void testRoundOnAvgOnSysTableColumn()
  {
    msqIncompatible();

    testBuilder()
        .sql("select round(avg(duration), 1) from sys.tasks ")
        .expectedResults(ImmutableList.of(
            new Object[]{8.3D}))
        .expectedLogicalPlan("LogicalProject(exprs=[[ROUND($0, 1)]])\n"
                             + "  LogicalAggregate(group=[{}], agg#0=[AVG($0)])\n"
                             + "    LogicalProject(exprs=[[$8]])\n"
                             + "      LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  /**
   * {@code sys.stack_trace} has no interpreter fallback, so a query that doesn't set
   * {@code useDruidEngineForSystemTables} is rejected before Calcite planning.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testStackTraceWithoutDruidEngineForSystemTables(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    testQueryThrows(
        """
        SELECT thread_name
        FROM sys.stack_trace
        WHERE server = 'localhost:8082'
        """,
        QueryContext.ofMap(QueryContextParameters.PLANNER_STRATEGY, plannerStrategy),
        DruidException.class,
        DruidExceptionMatcher.invalidInput().expectMessageIs(STACK_TRACE_REQUIRES_DRUID_ENGINE_MESSAGE)
    );
  }

  /**
   * Setting {@code useDruidEngineForSystemTables} isn't enough when {@code druid.query.enableDruidEngineForSystemTables}
   * is off, as it is in this test, because {@code sys.stack_trace} then has no datasource representation.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testStackTraceWithoutDruidEngineEnabled(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    testQueryThrows(
        """
        EXPLAIN PLAN FOR
        SELECT thread_name
        FROM sys.stack_trace
        WHERE server = 'localhost:8082'
        """,
        QueryContext.ofMap(
            QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
            true,
            QueryContextParameters.PLANNER_STRATEGY,
            plannerStrategy
        ),
        DruidException.class,
        DruidExceptionMatcher.invalidInput().expectMessageIs(STACK_TRACE_REQUIRES_DRUID_ENGINE_MESSAGE)
    );
  }

  /**
   * Subqueries stay as row expressions until planning, so the check must also look inside them.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testStackTraceInSubqueryWithoutDruidEngineEnabled(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final Map<String, Object> context = QueryContext.ofMap(
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
    for (final String sql : List.of(
        "SELECT dim1 FROM foo WHERE dim1 IN (SELECT thread_name FROM sys.stack_trace WHERE server = 'x')",
        "SELECT * FROM sys.servers WHERE server IN (SELECT server FROM sys.stack_trace WHERE server = 'x')",
        "SELECT dim1, (SELECT COUNT(*) FROM sys.stack_trace WHERE server = 'x') FROM foo",
        "EXPLAIN PLAN FOR SELECT dim1 FROM foo WHERE dim1 IN (SELECT thread_name FROM sys.stack_trace WHERE server = 'x')"
    )) {
      testQueryThrows(
          sql,
          context,
          DruidException.class,
          DruidExceptionMatcher.invalidInput().expectMessageIs(STACK_TRACE_REQUIRES_DRUID_ENGINE_MESSAGE)
      );
    }
  }
}
