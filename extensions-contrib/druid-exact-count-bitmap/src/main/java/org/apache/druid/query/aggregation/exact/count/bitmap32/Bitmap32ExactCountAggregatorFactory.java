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

package org.apache.druid.query.aggregation.exact.count.bitmap32;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Base class for both build and merge factories
 */
public abstract class Bitmap32ExactCountAggregatorFactory extends AggregatorFactory
{
  // 1KiB is large enough for bookkeeping plus some future head-room & small enough that we don’t waste direct memory.
  static final int MAX_INTERMEDIATE_SIZE = 1024; // 1 KiB
  static final Comparator<Bitmap32> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingLong(Bitmap32::getCardinality));

  @Nonnull
  private final String name;

  @Nonnull
  private final String fieldName;

  Bitmap32ExactCountAggregatorFactory(
      final String name,
      final String fieldName
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
  }

  @Override
  @Nonnull
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nonnull
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Nonnull
  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public Bitmap32 deserialize(final Object object)
  {
    return Bitmap32ExactCountMergeComplexMetricSerde.deserializeRoaringBitmap32Counter(object);
  }

  @Override
  public Bitmap32 combine(final Object objectA, final Object objectB)
  {
    if (objectB == null) {
      return (Bitmap32) objectA;
    }
    if (objectA == null) {
      return (Bitmap32) objectB;
    }
    return ((Bitmap32) objectA).fold((Bitmap32) objectB);
  }

  @Override
  public AggregateCombiner<Bitmap32> makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<>()
    {
      private Bitmap32 union = new RoaringBitmap32Counter();

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        union = new RoaringBitmap32Counter();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        final Bitmap32 bitmap32Counter = (Bitmap32) selector.getObject();
        union.fold(bitmap32Counter);
      }

      @Nullable
      @Override
      public Bitmap32 getObject()
      {
        return union;
      }

      @Override
      public Class<Bitmap32> classOfObject()
      {
        return Bitmap32.class;
      }
    };
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }
    return ((Bitmap32) object).getCardinality();
  }

  @Override
  public Comparator<Bitmap32> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new Bitmap32ExactCountMergeAggregatorFactory(getName(), getName());
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(getCacheTypeId()).appendString(name).appendString(fieldName).build();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return MAX_INTERMEDIATE_SIZE;
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().equals(object.getClass())) {
      return false;
    }
    final Bitmap32ExactCountAggregatorFactory that = (Bitmap32ExactCountAggregatorFactory) object;
    if (!name.equals(that.getName())) {
      return false;
    }
    return fieldName.equals(that.getFieldName());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + " {"
           + " name=" + name
           + ", fieldName=" + fieldName
           + " }";
  }

  protected abstract byte getCacheTypeId();

}
