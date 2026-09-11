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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ConverterRule} to convert {@link org.apache.calcite.rel.core.TableScan} to {@link DruidTableScan}
 */
public class DruidTableScanRule extends ConverterRule
{
  private final PlannerContext plannerContext;

  public DruidTableScanRule(
      final Class<? extends RelNode> clazz,
      final RelTrait in,
      final RelTrait out,
      final String descriptionPrefix,
      final PlannerContext plannerContext
  )
  {
    super(
        Config.INSTANCE
            .withConversion(clazz, in, out, descriptionPrefix)
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    final LogicalTableScan tableScan = call.rel(0);
    final RelOptTable table = tableScan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);

    // Native-only system tables must not enter the Druid logical convention unless native planning is enabled.
    return druidTable != null || SystemSchema.canUseNativeSystemTable(table, plannerContext);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel)
  {
    final LogicalTableScan tableScan = (LogicalTableScan) rel;
    final RelTraitSet newTrait = tableScan.getTraitSet().replace(DruidLogicalConvention.instance());
    final DruidTableScan druidTableScan = new DruidTableScan(
        tableScan.getCluster(),
        newTrait,
        tableScan.getTable()
    );
    return druidTableScan;
  }
}
