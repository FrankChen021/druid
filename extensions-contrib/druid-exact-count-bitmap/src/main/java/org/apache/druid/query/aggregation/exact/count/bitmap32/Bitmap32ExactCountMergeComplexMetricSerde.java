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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

public class Bitmap32ExactCountMergeComplexMetricSerde extends ComplexMetricSerde
{
  static RoaringBitmap32Counter deserializeRoaringBitmap32Counter(final Object object)
  {
    if (object instanceof String) {
      return RoaringBitmap32Counter.fromBytes(decodeStringToByteArray((String) object));
    } else if (object instanceof byte[]) {
      return RoaringBitmap32Counter.fromBytes((byte[]) object);
    } else if (object instanceof RoaringBitmap32Counter) {
      return (RoaringBitmap32Counter) object;
    }
    throw new IAE("Cannot deserialize type[%s] to an RoaringBitmap32Counter:", object.getClass().getName());
  }

  private static byte[] decodeStringToByteArray(String string)
  {
    try {
      return StringUtils.decodeBase64(StringUtils.toUtf8(string));
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Failed to deserialize to RoaringBitmap32Counter, input is an invalid base64 string");
    }
  }

  @Override
  public String getTypeName()
  {
    return Bitmap32ExactCountModule.TYPE_NAME; // must be common type name
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return Bitmap32ExactCountObjectStrategy.STRATEGY;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return RoaringBitmap32Counter.class;
      }

      @Override
      public RoaringBitmap32Counter extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null) {
          return null;
        }
        return deserializeRoaringBitmap32Counter(object);
      }
    };
  }

  @Override
  public void deserializeColumn(final ByteBuffer buf, final ColumnBuilder columnBuilder)
  {
    columnBuilder.setComplexColumnSupplier(
        new ComplexColumnPartSupplier(
            getTypeName(),
            GenericIndexed.read(buf, Bitmap32ExactCountObjectStrategy.STRATEGY, columnBuilder.getFileMapper())
        )
    );
  }

  // support large columns
  @Override
  public GenericColumnSerializer getSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
