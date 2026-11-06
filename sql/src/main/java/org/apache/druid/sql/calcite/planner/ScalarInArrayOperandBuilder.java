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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.druid.sql.calcite.expression.builtin.ScalarInArrayOperatorConversion;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Builds the array operand of a {@link ScalarInArrayOperatorConversion} call that {@link DruidSqlValidator} rewrote
 * from a large IN list.
 *
 * When all values are character literals or all are numeric literals, the values are moved out of the SQL tree into
 * a synthesized dynamic parameter registered with {@link ScalarInArrayDynamicParameters}.
 * Calcite then validates, converts and optimizes a single parameter rather than one literal per value.
 * Otherwise, the values are wrapped in an {@code ARRAY[...]} constructor which Calcite will perform more work that
 * slows the performance.
 *
 * Since the synthesized parameters are not parameters of the original statement, this class also recognizes them for
 * {@link DruidSqlValidator}, which must skip them during type inference and when reporting parameter types.
 */
public class ScalarInArrayOperandBuilder
{
  private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

  private final SqlValidator validator;
  private final ScalarInArrayDynamicParameters parameters;

  public ScalarInArrayOperandBuilder(final SqlValidator validator, final ScalarInArrayDynamicParameters parameters)
  {
    this.validator = validator;
    this.parameters = parameters;
  }

  /**
   * Returns the array operand for {@code exprNode IN valuesNode}: a synthesized parameter if possible, or an
   * {@code ARRAY[...]} constructor otherwise. The constructor is also used when the IN expression references no
   * columns, so that Calcite can still reduce it to a constant.
   */
  public SqlNode toArrayOperand(final SqlNode exprNode, final SqlNodeList valuesNode)
  {
    final SqlTypeName elementTypeName = getElementType(valuesNode);
    if (elementTypeName == null || !containsIdentifier(exprNode)) {
      return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(valuesNode);
    }

    final List<Object> values = new ArrayList<>(valuesNode.size());
    for (final SqlNode valueNode : valuesNode) {
      final SqlLiteral literal = (SqlLiteral) valueNode;
      // The literals never reach validation once moved into the parameter, so validate them here.
      validator.validateLiteral(literal);
      if (elementTypeName == SqlTypeName.VARCHAR) {
        values.add(literal.getValueAs(String.class));
      } else if (elementTypeName == SqlTypeName.BIGINT) {
        values.add(literal.getValueAs(Long.class));
      } else {
        // Same conversion as Expressions#calciteLiteralToDruidLiteral for non-integer numeric literals.
        values.add(literal.getValueAs(BigDecimal.class).doubleValue());
      }
    }

    final SqlDynamicParam param = new SqlDynamicParam(parameters.add(values), valuesNode.getParserPosition());
    // The type is set here, since the validator cannot infer the type of dynamic parameter from its context.
    validator.setValidatedNodeType(
        param,
        Calcites.createSqlArrayTypeWithNullability(validator.getTypeFactory(), elementTypeName, false)
    );
    return param;
  }

  /**
   * Whether this node is a dynamic parameter synthesized by {@link #toArrayOperand(SqlNode, SqlNodeList)}.
   */
  public boolean isSynthesizedParameter(final SqlNode node)
  {
    return node instanceof SqlDynamicParam && parameters.get(((SqlDynamicParam) node).getIndex()) != null;
  }

  /**
   * Same as {@link org.apache.calcite.sql.validate.SqlValidatorImpl#getParameterRowType(SqlNode)}, but excludes
   * synthesized parameters, since they are not parameters of the original statement.
   */
  public RelDataType getParameterRowType(final SqlNode sqlQuery)
  {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    final Set<SqlNode> alreadyVisited = Collections.newSetFromMap(new IdentityHashMap<>());
    sqlQuery.accept(
        new SqlShuttle()
        {
          @Override
          public SqlNode visit(final SqlDynamicParam param)
          {
            if (!isSynthesizedParameter(param) && alreadyVisited.add(param)) {
              names.add("?" + types.size());
              types.add(validator.getValidatedNodeType(param));
            }
            return param;
          }
        }
    );
    return validator.getTypeFactory().createStructType(types, names);
  }

  /**
   * Returns the element type of the synthesized parameter for an IN list, or null if the list should keep the
   * {@code ARRAY[...]} form:
   *
   * <ul>
   *   <li>{@link SqlTypeName#VARCHAR} if all values are character literals.</li>
   *   <li>{@link SqlTypeName#BIGINT} if all values are exact integer literals that fit in a long.</li>
   *   <li>{@link SqlTypeName#DOUBLE} if all values are numeric literals and at least one is not such an integer.
   *   This matches the {@code ARRAY[...]} form, whose element type is then DECIMAL or DOUBLE, both translated to
   *   double values.</li>
   * </ul>
   */
  @Nullable
  private static SqlTypeName getElementType(final SqlNodeList valuesNode)
  {
    final SqlNode first = valuesNode.getFirst();
    if (first instanceof SqlCharStringLiteral) {
      for (final SqlNode valueNode : valuesNode) {
        if (!(valueNode instanceof SqlCharStringLiteral)) {
          return null;
        }
      }
      return SqlTypeName.VARCHAR;
    } else if (first instanceof SqlNumericLiteral) {
      boolean allLongs = true;
      for (final SqlNode valueNode : valuesNode) {
        if (!(valueNode instanceof SqlNumericLiteral)) {
          return null;
        }
        if (allLongs && !isLongLiteral((SqlNumericLiteral) valueNode)) {
          allLongs = false;
        }
      }
      return allLongs ? SqlTypeName.BIGINT : SqlTypeName.DOUBLE;
    } else {
      return null;
    }
  }

  private static boolean isLongLiteral(final SqlNumericLiteral literal)
  {
    if (!literal.isExact() || !literal.isInteger()) {
      return false;
    }
    final BigDecimal value = literal.getValueAs(BigDecimal.class);
    return value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0;
  }

  /**
   * Whether an expression contains an identifier that is not a niladic function call such as
   * {@code CURRENT_TIMESTAMP}, which the parser also represents as an identifier.
   */
  private boolean containsIdentifier(final SqlNode node)
  {
    final boolean[] found = {false};
    node.accept(
        new SqlBasicVisitor<Void>()
        {
          @Override
          public Void visit(final SqlIdentifier id)
          {
            if (validator.makeNullaryCall(id) == null) {
              found[0] = true;
            }
            return null;
          }
        }
    );
    return found[0];
  }
}
