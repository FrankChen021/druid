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

import com.google.common.primitives.Longs;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;

/**
 * This class is marked as abstract to prevent usage of direct type instantiation.
 * To use it, just create an instance as below
 *
 * AggregateCombiner combiner = new GenericFirstAggregateCombiner<SerializablePair<Long, YOUR_DATA_TYPE>>(){
 * };
 *
 */
abstract public class GenericFirstAggregateCombiner<T extends SerializablePair<Long, ?>> extends ObjectAggregateCombiner<T>
{
  private T firstValue;

  @Override
  public final void reset(ColumnValueSelector selector)
  {
    firstValue = (T) selector.getObject();
  }

  @Override
  public final void fold(ColumnValueSelector selector)
  {
    T newValue = (T) selector.getObject();

    if (Longs.compare(((SerializablePair<Long, ?>) firstValue).lhs, ((SerializablePair<Long, ?>) newValue).lhs) > 0) {
      firstValue = newValue;
    }
  }

  @Nullable
  @Override
  public final T getObject()
  {
    return firstValue;
  }

  @Override
  public final Class<T> classOfObject()
  {
    ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
    return (Class<T>) parameterizedType.getActualTypeArguments()[0];
  }
}
