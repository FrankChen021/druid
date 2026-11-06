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

import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * Values of dynamic parameters synthesized for large IN lists, so that the values do not have to be carried through
 * Calcite as one literal per element. See {@link ScalarInArrayOperandBuilder} for how they are created.
 *
 * The values are read back when translating to native queries, rather than being bound to literals by
 * {@link RelParameterizerShuttle}.
 */
public class ScalarInArrayDynamicParameters
{
  private final IntSupplier firstIndexSupplier;
  private final Map<Integer, List<Object>> valuesByIndex = new HashMap<>();
  private final Map<List<Object>, Integer> indexByValues = new HashMap<>();

  // index of the next synthesized parameter, or -1 if not yet computed
  private int nextIndex = -1;

  /**
   * @param firstIndexSupplier index of the first synthesized parameter, which must be after every parameter of the
   *                           original statement. Called at most once, when the first parameter is added.
   */
  public ScalarInArrayDynamicParameters(final IntSupplier firstIndexSupplier)
  {
    this.firstIndexSupplier = firstIndexSupplier;
  }

  /**
   * Returns the index of the first parameter that cannot collide with the parameters of a statement.
   *
   * @param sqlNode             the parsed statement
   * @param boundParameterCount number of parameter values bound to the statement
   */
  public static int firstAvailableIndex(@Nullable final SqlNode sqlNode, final int boundParameterCount)
  {
    final int[] maxIndex = {-1};
    if (sqlNode != null) {
      sqlNode.accept(
          new SqlBasicVisitor<Void>()
          {
            @Override
            public Void visit(final SqlDynamicParam param)
            {
              maxIndex[0] = Math.max(maxIndex[0], param.getIndex());
              return null;
            }
          }
      );
    }
    return Math.max(boundParameterCount, maxIndex[0] + 1);
  }

  /**
   * Registers the values of an IN list and returns the index of the synthesized parameter that holds them.
   *
   * Identical lists of values share one parameter, so that repeated IN expressions remain equal to each other, for
   * example when an expression in the SELECT list must match the same expression in GROUP BY. Lists of values of
   * different types are never equal, since the values are Strings, Longs, or Doubles.
   */
  public int add(final List<Object> values)
  {
    final Integer existingIndex = indexByValues.get(values);
    if (existingIndex != null) {
      return existingIndex;
    }
    if (nextIndex < 0) {
      nextIndex = firstIndexSupplier.getAsInt();
    }
    final int index = nextIndex++;
    valuesByIndex.put(index, values);
    indexByValues.put(values, index);
    return index;
  }

  /**
   * Returns the values of the synthesized parameter at this index, or null if the parameter at this index was not
   * synthesized for an IN list.
   */
  @Nullable
  public List<Object> get(final int index)
  {
    return valuesByIndex.get(index);
  }

  /**
   * Returns the values if this node is a synthesized parameter, or null otherwise.
   */
  @Nullable
  public List<Object> get(final RexNode node)
  {
    return node instanceof RexDynamicParam ? get(((RexDynamicParam) node).getIndex()) : null;
  }
}
