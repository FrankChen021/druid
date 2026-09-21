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

package org.apache.druid.segment.virtual;

import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

public class SingleStringInputDictionaryEncodedExpressionVectorObjectSelectorTest
    extends InitializedNullHandlingTest
{
  @Test
  public void testEvaluatesEachDictionaryValueOnce()
  {
    final TestVectorInspector vectorInspector = new TestVectorInspector();
    final TestDimensionSelector dimensionSelector = new TestDimensionSelector(vectorInspector);
    final SingleStringInputDictionaryEncodedExpressionVectorObjectSelector selector =
        new SingleStringInputDictionaryEncodedExpressionVectorObjectSelector(
            vectorInspector,
            dimensionSelector,
            Parser.parse("upper(x)", ExprMacroTable.nil())
        );

    Assertions.assertArrayEquals(
        new Object[]{"ALPHA", "BETA", "ALPHA", null},
        selector.getObjectVector()
    );
    Assertions.assertArrayEquals(new int[]{1, 1, 1}, dimensionSelector.lookupCounts);

    vectorInspector.id++;
    dimensionSelector.rows = new int[]{1, 2, 1, 0};

    Assertions.assertArrayEquals(
        new Object[]{"BETA", null, "BETA", "ALPHA"},
        selector.getObjectVector()
    );
    Assertions.assertArrayEquals(new int[]{1, 1, 1}, dimensionSelector.lookupCounts);
  }

  @Test
  public void testLargeDictionaryDoesNotRetainEvaluatedValues()
  {
    final TestVectorInspector vectorInspector = new TestVectorInspector();
    final TestDimensionSelector dimensionSelector = new TestDimensionSelector(
        vectorInspector,
        SingleStringInputDictionaryEncodedExpressionVectorObjectSelector.MAX_DICTIONARY_SIZE + 1
    );
    final SingleStringInputDictionaryEncodedExpressionVectorObjectSelector selector =
        new SingleStringInputDictionaryEncodedExpressionVectorObjectSelector(
            vectorInspector,
            dimensionSelector,
            Parser.parse("upper(x)", ExprMacroTable.nil())
        );

    Assertions.assertArrayEquals(
        new Object[]{"ALPHA", "BETA", "ALPHA", null},
        selector.getObjectVector()
    );

    vectorInspector.id++;
    Assertions.assertArrayEquals(
        new Object[]{"ALPHA", "BETA", "ALPHA", null},
        selector.getObjectVector()
    );
    Assertions.assertArrayEquals(new int[]{4, 2, 2}, dimensionSelector.lookupCounts);
  }

  private static class TestVectorInspector implements ReadableVectorInspector
  {
    private int id;

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public int getMaxVectorSize()
    {
      return 4;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return 4;
    }
  }

  private static class TestDimensionSelector implements SingleValueDimensionVectorSelector
  {
    private static final String[] DICTIONARY = {"alpha", "beta", null};

    private final TestVectorInspector vectorInspector;
    private final int cardinality;
    private final int[] lookupCounts = new int[DICTIONARY.length];
    private int[] rows = {0, 1, 0, 2};

    private TestDimensionSelector(TestVectorInspector vectorInspector)
    {
      this(vectorInspector, DICTIONARY.length);
    }

    private TestDimensionSelector(TestVectorInspector vectorInspector, int cardinality)
    {
      this.vectorInspector = vectorInspector;
      this.cardinality = cardinality;
    }

    @Override
    public int[] getRowVector()
    {
      return rows;
    }

    @Override
    public int getValueCardinality()
    {
      return cardinality;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      lookupCounts[id]++;
      return DICTIONARY[id];
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return null;
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorInspector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return vectorInspector.getCurrentVectorSize();
    }
  }
}
