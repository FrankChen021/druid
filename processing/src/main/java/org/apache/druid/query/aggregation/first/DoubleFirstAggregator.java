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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class DoubleFirstAggregator extends NumericFirstAggregator
{
  double firstValue;

  public DoubleFirstAggregator(
      BaseLongColumnValueSelector timeSelector,
      ColumnValueSelector valueSelector,
      boolean needsFoldCheck
  )
  {
    super(timeSelector, valueSelector, needsFoldCheck);
    firstValue = 0;
  }

  @Override
  void setFirstValue(ColumnValueSelector valueSelector)
  {
    firstValue = valueSelector.getDouble();
  }

  @Override
  void setFirstValue(Number firstValue)
  {
    this.firstValue = firstValue.doubleValue();
  }

  @Override
  Number getFirstValue()
  {
    return firstValue;
  }

  @Override
  public SerializablePair<Long, ? extends Number> getPairObject(long firstTime, @Nullable Number firstValue)
  {
    return new SerializablePairLongDouble(firstTime, (Double) firstValue);
  }

  @Override
  public float getFloat()
  {
    return (float) firstValue;
  }

  @Override
  public double getDouble()
  {
    return firstValue;
  }

  @Override
  public long getLong()
  {
    return (long) firstValue;
  }
}

