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

package org.apache.druid.query.aggregation.last;

import com.google.common.primitives.Longs;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class GenericLastAggregateCombiner<T extends SerializablePair<Long, ?>>
    extends ObjectAggregateCombiner<T>
{
  private T lastValue;

  private final Class<T> pairClass;

  public GenericLastAggregateCombiner(Class<T> pairClass)
  {
    this.pairClass = pairClass;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    lastValue = (T) selector.getObject();
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    T newValue = (T) selector.getObject();

    if (Longs.compare(lastValue.lhs, newValue.lhs) <= 0) {
      lastValue = newValue;
    }
  }

  @Nullable
  @Override
  public T getObject()
  {
    return lastValue;
  }

  @Override
  public Class<T> classOfObject()
  {
    return this.pairClass;
  }
}
