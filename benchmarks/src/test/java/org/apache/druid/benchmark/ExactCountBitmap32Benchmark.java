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

package org.apache.druid.benchmark;

import org.apache.druid.query.aggregation.exact.count.bitmap32.Bitmap32;
import org.apache.druid.query.aggregation.exact.count.bitmap32.Bitmap32ExactCountBuildBufferAggregator;
import org.apache.druid.query.aggregation.exact.count.bitmap32.Bitmap32ExactCountMergeBufferAggregator;
import org.apache.druid.query.aggregation.exact.count.bitmap32.Bitmap32ExactCountObjectStrategy;
import org.apache.druid.query.aggregation.exact.count.bitmap32.RoaringBitmap32Counter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for the bitmap32 exact count extension. Serialized inputs live in one direct buffer and are read through
 * the complex column object strategy, the way merged column values are read from memory mapped segments.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExactCountBitmap32Benchmark
{
  private static final int NUM_GROUPS = 500;
  private static final int NUM_ROWS = 20_000;

  @Param({"1000", "10000"})
  public int numBitmaps;

  /**
   * sparse: a few random values per bitmap, dense: a contiguous run of values per bitmap
   */
  @Param({"sparse", "dense"})
  public String shape;

  private final Bitmap32ExactCountObjectStrategy strategy = new Bitmap32ExactCountObjectStrategy();

  private ByteBuffer column;
  private int[] offsets;
  private int[] lengths;
  private int[] rowGroup;
  private int[] rowToBitmap;
  private long[] rowValues;
  private RoaringBitmap32Counter mergedCounter;

  @Setup(Level.Trial)
  public void setup()
  {
    final Random random = new Random(1234);
    final byte[][] serialized = new byte[numBitmaps][];
    offsets = new int[numBitmaps];
    lengths = new int[numBitmaps];
    int total = 0;
    for (int i = 0; i < numBitmaps; i++) {
      final RoaringBitmap32Counter counter = new RoaringBitmap32Counter();
      if ("sparse".equals(shape)) {
        for (int j = 0; j < 3; j++) {
          counter.add(1 + random.nextInt(Integer.MAX_VALUE - 1));
        }
      } else {
        final int start = random.nextInt(1 << 22);
        for (int v = start; v < start + 4096; v++) {
          counter.add(v + 1);
        }
      }
      serialized[i] = strategy.toBytes(counter);
      offsets[i] = total;
      lengths[i] = serialized[i].length;
      total += lengths[i];
    }
    column = ByteBuffer.allocateDirect(total);
    for (byte[] bytes : serialized) {
      column.put(bytes);
    }

    rowGroup = new int[NUM_ROWS];
    rowToBitmap = new int[NUM_ROWS];
    rowValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      rowGroup[i] = random.nextInt(NUM_GROUPS);
      rowToBitmap[i] = random.nextInt(numBitmaps);
      rowValues[i] = 1 + random.nextInt(Integer.MAX_VALUE - 1);
    }

    mergedCounter = mergeAll();
  }

  private Bitmap32 read(int i)
  {
    final ByteBuffer view = column.duplicate();
    view.position(offsets[i]);
    return strategy.fromByteBuffer(view, lengths[i]);
  }

  private RoaringBitmap32Counter mergeAll()
  {
    final RoaringBitmap32Counter union = new RoaringBitmap32Counter();
    for (int i = 0; i < numBitmaps; i++) {
      union.fold(read(i));
    }
    return union;
  }

  /**
   * Reading column values only, no merging.
   */
  @Benchmark
  public void bitmap32Deserialize(Blackhole blackhole)
  {
    for (int i = 0; i < numBitmaps; i++) {
      blackhole.consume(read(i));
    }
  }

  /**
   * Read every value and fold it into one accumulator, then read the cardinality: the core of a merge query.
   */
  @Benchmark
  public void bitmap32ReadFoldCardinality(Blackhole blackhole)
  {
    blackhole.consume(mergeAll().getCardinality());
  }

  /**
   * A merged value shipped to another node: read, fold, then serialize through the object strategy.
   */
  @Benchmark
  public void bitmap32ReadFoldSerialize(Blackhole blackhole)
  {
    blackhole.consume(strategy.toBytes(mergeAll()));
  }

  /**
   * Serialize an already merged value.
   */
  @Benchmark
  public void bitmap32SerializeMerged(Blackhole blackhole)
  {
    blackhole.consume(strategy.toBytes(mergedCounter));
  }

  /**
   * GroupBy style merge: rows read from the column hit random groups of a BufferAggregator, then every group is read.
   */
  @Benchmark
  public void bitmap32MergeBufferAggregator(Blackhole blackhole)
  {
    final int[] current = new int[1];
    final Bitmap32ExactCountMergeBufferAggregator aggregator = new Bitmap32ExactCountMergeBufferAggregator(
        new ObjectColumnSelector<Bitmap32>()
        {
          @Override
          public Bitmap32 getObject()
          {
            return read(current[0]);
          }

          @Override
          public Class<Bitmap32> classOfObject()
          {
            return Bitmap32.class;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
          }
        }
    );
    final ByteBuffer buf = ByteBuffer.allocate(8 * NUM_GROUPS);
    for (int g = 0; g < NUM_GROUPS; g++) {
      aggregator.init(buf, g * 8);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      current[0] = rowToBitmap[i];
      aggregator.aggregate(buf, rowGroup[i] * 8);
    }
    long total = 0;
    for (int g = 0; g < NUM_GROUPS; g++) {
      total += ((Bitmap32) aggregator.get(buf, g * 8)).getCardinality();
    }
    blackhole.consume(total);
  }

  @Benchmark
  public void bitmap32BuildBufferAggregator(Blackhole blackhole)
  {
    final long[] current = new long[1];
    final Bitmap32ExactCountBuildBufferAggregator aggregator = new Bitmap32ExactCountBuildBufferAggregator(
        new BaseLongColumnValueSelector()
        {
          @Override
          public long getLong()
          {
            return current[0];
          }

          @Override
          public boolean isNull()
          {
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
          }
        }
    );
    final ByteBuffer buf = ByteBuffer.allocate(8 * NUM_GROUPS);
    for (int g = 0; g < NUM_GROUPS; g++) {
      aggregator.init(buf, g * 8);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      current[0] = rowValues[i];
      aggregator.aggregate(buf, rowGroup[i] * 8);
    }
    long total = 0;
    for (int g = 0; g < NUM_GROUPS; g++) {
      total += ((Bitmap32) aggregator.get(buf, g * 8)).getCardinality();
    }
    blackhole.consume(total);
  }
}
