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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.SystemStackTraceTable;
import org.mockito.Mockito;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DruidTableScanRuleTest
{
  private RelOptCluster cluster;

  @BeforeEach
  public void setUp()
  {
    final RexBuilder rexBuilder = new RexBuilder(DruidTypeSystem.TYPE_FACTORY);
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
  }

  @Test
  public void testDoesNotConvertNativeOnlySystemTableWhenNativePlanningIsDisabled()
  {
    final PlannerContext plannerContext = createPlannerContext(false, NativeSqlEngine.NAME);
    final LogicalTableScan scan = createStackTraceScan();
    final RelOptRuleCall call = createRuleCall(scan);

    new DruidTableScanRule(plannerContext).onMatch(call);

    Mockito.verify(call, Mockito.never()).transformTo(Mockito.any(RelNode.class));
  }

  @Test
  public void testDoesNotConvertNativeOnlySystemTableForNonNativeEngine()
  {
    final PlannerContext plannerContext = createPlannerContext(true, "non-native");
    final LogicalTableScan scan = createStackTraceScan();
    final RelOptRuleCall call = createRuleCall(scan);

    new DruidTableScanRule(plannerContext).onMatch(call);

    Mockito.verify(call, Mockito.never()).transformTo(Mockito.any(RelNode.class));
  }

  @Test
  public void testConvertsNativeOnlySystemTableForNativeEngineWhenEnabled()
  {
    final PlannerContext plannerContext = createPlannerContext(true, NativeSqlEngine.NAME);
    final LogicalTableScan scan = createStackTraceScan();
    final RelOptRuleCall call = createRuleCall(scan);

    new DruidTableScanRule(plannerContext).onMatch(call);

    Mockito.verify(call).transformTo(Mockito.any(DruidQueryRel.class));
  }

  private PlannerContext createPlannerContext(final boolean useNative, final String engineName)
  {
    final SqlEngine engine = Mockito.mock(SqlEngine.class);
    Mockito.when(engine.name()).thenReturn(engineName);
    final PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.useNativeQueryForSystemTables()).thenReturn(useNative);
    Mockito.when(plannerContext.getEngine()).thenReturn(engine);
    return plannerContext;
  }

  private LogicalTableScan createStackTraceScan()
  {
    final SystemStackTraceTable table = new SystemStackTraceTable();
    final RelOptTable relOptTable = Mockito.mock(RelOptTable.class);
    final RelDataType rowType = table.getRowType(DruidTypeSystem.TYPE_FACTORY);
    Mockito.when(relOptTable.getRowType()).thenReturn(rowType);
    Mockito.when(relOptTable.getQualifiedName()).thenReturn(List.of("sys", "stack_trace"));
    Mockito.when(relOptTable.unwrap(Mockito.any())).thenAnswer(invocation -> {
      final Class<?> requestedClass = invocation.getArgument(0);
      return requestedClass.isInstance(table) ? table : null;
    });
    return LogicalTableScan.create(cluster, relOptTable, List.of());
  }

  private RelOptRuleCall createRuleCall(final LogicalTableScan scan)
  {
    final RelOptRuleCall call = Mockito.mock(RelOptRuleCall.class);
    Mockito.when(call.<LogicalTableScan>rel(0)).thenReturn(scan);
    return call;
  }
}
