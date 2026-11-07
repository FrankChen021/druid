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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.expression.builtin.ScalarInArrayOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidRexExecutor;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.ScalarInArrayDynamicParameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reduces {@code SCALAR_IN_ARRAY(constant, ?k)} to a constant, where {@code ?k} is a dynamic parameter synthesized for
 * a large IN list (see {@link ScalarInArrayDynamicParameters}).
 *
 * Calcite's {@link org.apache.calcite.rel.rules.ReduceExpressionsRule} treats dynamic parameters as variables, so it
 * cannot reduce these calls, unlike the equivalent {@code SCALAR_IN_ARRAY(constant, ARRAY[...])}. The left-hand side
 * may become constant during planning, for example when a predicate from a subquery or view is substituted into it.
 * The call is evaluated with {@link DruidRexExecutor}, as {@code ReduceExpressionsRule} would evaluate the
 * {@code ARRAY[...]} form.
 */
public class ReduceScalarInArrayParameterRule extends RelOptRule implements SubstitutionRule
{
  private final PlannerContext plannerContext;

  public ReduceScalarInArrayParameterRule(final PlannerContext plannerContext)
  {
    super(operand(RelNode.class, any()));
    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    if (plannerContext.getScalarInArrayDynamicParameters().isEmpty()) {
      return;
    }

    final RelNode oldNode = call.rel(0);
    final RexBuilder rexBuilder = oldNode.getCluster().getRexBuilder();
    final RelNode newNode = oldNode.accept(new ReduceShuttle(rexBuilder));

    //noinspection ObjectEquality
    if (newNode != oldNode) {
      final RelBuilder relBuilder = call.builder().push(newNode);
      relBuilder.convert(oldNode.getRowType(), false);
      call.transformTo(relBuilder.build());
      call.getPlanner().prune(oldNode);
    }
  }

  private class ReduceShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    ReduceShuttle(final RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call)
    {
      final RexNode visited = super.visitCall(call);
      if (!isReducible(visited)) {
        return visited;
      }

      final List<RexNode> reduced = new ArrayList<>(1);
      new DruidRexExecutor(plannerContext).reduce(rexBuilder, Collections.singletonList(visited), reduced);
      // DruidRexExecutor may cast the result to the nullable type of the call, which Filter conditions do not allow.
      return RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), reduced.get(0));
    }

    /**
     * Whether a node is {@code SCALAR_IN_ARRAY(constant, ?k)} with a synthesized parameter {@code ?k}.
     */
    private boolean isReducible(final RexNode node)
    {
      if (!(node instanceof RexCall)
          || !ScalarInArrayOperatorConversion.SQL_FUNCTION.equals(((RexCall) node).getOperator())) {
        return false;
      }
      final List<RexNode> operands = ((RexCall) node).getOperands();
      return plannerContext.getScalarInArrayDynamicParameters().get(operands.get(1)) != null
             && RexUtil.isConstant(operands.get(0));
    }
  }
}
