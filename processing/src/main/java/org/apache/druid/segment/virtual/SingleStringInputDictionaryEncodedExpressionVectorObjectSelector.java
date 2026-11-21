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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

/**
 * Evaluates a deterministic, single-input string expression once per dictionary value for small dictionaries.
 * Large dictionaries use the same batched expression evaluation without retaining results across vectors.
 */
public class SingleStringInputDictionaryEncodedExpressionVectorObjectSelector implements VectorObjectSelector
{
  // Match the bounded dictionary cache used by SingleStringInputCachingExpressionColumnValueSelector.
  static final int MAX_DICTIONARY_SIZE = 1_000;

  private final ReadableVectorInspector vectorInspector;
  private final SingleValueDimensionVectorSelector selector;
  private final ExprVectorProcessor<Object[]> stringProcessor;
  private final DictionaryInputBinding inputBinding;
  private final boolean cacheDictionary;
  @Nullable
  private final Object[] dictionaryValues;
  // 0 means unseen, 1 means queued in the current batch, and 2 means evaluated and cached.
  @Nullable
  private final byte[] dictionaryStates;
  private final int[] pendingIds;
  private final Object[] output;

  private int currentVectorId = ReadableVectorInspector.NULL_ID;
  private Object[] currentOutput;

  public SingleStringInputDictionaryEncodedExpressionVectorObjectSelector(
      ReadableVectorInspector vectorInspector,
      SingleValueDimensionVectorSelector selector,
      Expr expression
  )
  {
    final int cardinality = selector.getValueCardinality();
    if (cardinality == DimensionDictionarySelector.CARDINALITY_UNKNOWN || !selector.nameLookupPossibleInAdvance()) {
      throw new ISE(
          "Selector of class[%s] does not have an advance-readable dictionary, cannot use it.",
          selector.getClass().getName()
      );
    }

    this.vectorInspector = vectorInspector;
    this.selector = selector;
    this.inputBinding = new DictionaryInputBinding(selector.getMaxVectorSize());
    this.stringProcessor = expression.asVectorProcessor(inputBinding);
    this.cacheDictionary = cardinality <= MAX_DICTIONARY_SIZE;
    this.dictionaryValues = cacheDictionary ? new Object[cardinality] : null;
    this.dictionaryStates = cacheDictionary ? new byte[cardinality] : null;
    this.pendingIds = new int[selector.getMaxVectorSize()];
    this.output = new Object[selector.getMaxVectorSize()];
    this.currentOutput = output;
  }

  @Override
  public Object[] getObjectVector()
  {
    if (vectorInspector.getId() == currentVectorId) {
      return currentOutput;
    }

    final int[] rows = selector.getRowVector();
    final int currentSize = selector.getCurrentVectorSize();

    if (!cacheDictionary) {
      for (int i = 0; i < currentSize; i++) {
        inputBinding.values[i] = selector.lookupName(rows[i]);
      }
      inputBinding.setCurrentSize(currentSize);
      currentOutput = stringProcessor.evalVector(inputBinding).getObjectVector();
      currentVectorId = vectorInspector.getId();
      return currentOutput;
    }

    assert dictionaryValues != null;
    assert dictionaryStates != null;
    int pendingCount = 0;

    for (int i = 0; i < currentSize; i++) {
      final int dictionaryId = rows[i];
      if (dictionaryStates[dictionaryId] == 0) {
        // Mark it immediately so duplicate rows in this batch are added only once.
        dictionaryStates[dictionaryId] = 1;
        pendingIds[pendingCount] = dictionaryId;
        inputBinding.values[pendingCount] = selector.lookupName(dictionaryId);
        pendingCount++;
      }
    }

    if (pendingCount > 0) {
      inputBinding.setCurrentSize(pendingCount);
      boolean evaluationSucceeded = false;
      try {
        final Object[] evaluatedValues = stringProcessor.evalVector(inputBinding).getObjectVector();
        for (int i = 0; i < pendingCount; i++) {
          dictionaryValues[pendingIds[i]] = evaluatedValues[i];
          dictionaryStates[pendingIds[i]] = 2;
        }
        evaluationSucceeded = true;
      }
      finally {
        if (!evaluationSucceeded) {
          for (int i = 0; i < pendingCount; i++) {
            dictionaryStates[pendingIds[i]] = 0;
          }
        }
      }
    }

    for (int i = 0; i < currentSize; i++) {
      output[i] = dictionaryValues[rows[i]];
    }

    currentOutput = output;
    currentVectorId = vectorInspector.getId();
    return currentOutput;
  }

  @Override
  public int getMaxVectorSize()
  {
    return selector.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return selector.getCurrentVectorSize();
  }

  private static class DictionaryInputBinding implements Expr.VectorInputBinding
  {
    private final Object[] values;
    private int currentSize;
    private int currentVectorId;

    private DictionaryInputBinding(int maxVectorSize)
    {
      this.values = new Object[maxVectorSize];
    }

    private void setCurrentSize(int currentSize)
    {
      this.currentSize = currentSize;
      currentVectorId++;
    }

    @Nullable
    @Override
    public ExpressionType getType(String name)
    {
      return ExpressionType.STRING;
    }

    @Override
    public int getMaxVectorSize()
    {
      return values.length;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentSize;
    }

    @Override
    public int getCurrentVectorId()
    {
      return currentVectorId;
    }

    @Override
    public Object[] getObjectVector(String name)
    {
      return values;
    }

    @Override
    public long[] getLongVector(String name)
    {
      throw new UnsupportedOperationException("attempt to get long[] from string[] only binding");
    }

    @Override
    public double[] getDoubleVector(String name)
    {
      throw new UnsupportedOperationException("attempt to get double[] from string[] only binding");
    }

    @Nullable
    @Override
    public boolean[] getNullVector(String name)
    {
      throw new UnsupportedOperationException("attempt to get boolean[] null vector from string[] only binding");
    }
  }
}
