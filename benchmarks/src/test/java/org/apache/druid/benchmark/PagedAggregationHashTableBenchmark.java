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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.AdaptivePagedAggregationHashTable;
import org.apache.druid.query.groupby.epinephelinae.BlockingPoolMergeMemoryBackingAllocator;
import org.apache.druid.query.groupby.epinephelinae.BufferHashGrouper;
import org.apache.druid.query.groupby.epinephelinae.GroupByTestColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.query.groupby.epinephelinae.IntKey;
import org.apache.druid.query.groupby.epinephelinae.MergeMemoryLease;
import org.apache.druid.query.groupby.epinephelinae.MergeMemoryManager;
import org.apache.druid.query.groupby.epinephelinae.PagedAggregationHashTable;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class PagedAggregationHashTableBenchmark
{
  private static final int BUFFER_SIZE = 32 * 1024 * 1024;
  private static final int ROW_COUNT = 1_000_000;
  private static final double ZIPF_EXPONENT = 1.2;

  @Param({"1", "16", "256", "1000", "5000", "10000", "100000", "500000"})
  public int cardinality;

  // Other investigation values: uniform, zipf.
  @Param({"cyclic"})
  public String distribution;

  // Other investigation values: countSum, sum, count.
  @Param({"sumCount"})
  public String aggregation;

  private ByteBuffer legacyBuffer;
  private MergeMemoryLease pagedLease;
  private MergeMemoryLease adaptiveLease;
  private BufferHashGrouper<IntKey> legacy;
  private PagedAggregationHashTable<IntKey> paged;
  private AdaptivePagedAggregationHashTable<IntKey> adaptive;
  private IntKey[] distinctKeys;
  private IntKey[] updateKeys;

  @Setup(Level.Trial)
  public void setup()
  {
    final GroupByTestColumnSelectorFactory legacySelectors = GrouperTestUtil.newColumnSelectorFactory();
    final GroupByTestColumnSelectorFactory pagedSelectors = GrouperTestUtil.newColumnSelectorFactory();
    final GroupByTestColumnSelectorFactory adaptiveSelectors = GrouperTestUtil.newColumnSelectorFactory();
    legacySelectors.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    pagedSelectors.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    adaptiveSelectors.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    legacyBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    final int backingBufferSize = 8 * 1024 * 1024;
    final DefaultBlockingPool<ByteBuffer> pool = new DefaultBlockingPool<>(
        () -> ByteBuffer.allocateDirect(backingBufferSize),
        2 * BUFFER_SIZE / backingBufferSize
    );
    final MergeMemoryManager manager = new MergeMemoryManager(
        new BlockingPoolMergeMemoryBackingAllocator(pool, backingBufferSize),
        1024 * 1024
    );
    pagedLease = manager.acquireMinimum(new QueryResourceId("benchmark"), 2, 32, 0);
    adaptiveLease = manager.acquireMinimum(new QueryResourceId("adaptive-benchmark"), 2, 32, 0);
    legacy = new BufferHashGrouper<>(
        Suppliers.ofInstance(legacyBuffer),
        GrouperTestUtil.intKeySerde(),
        aggregators(legacySelectors),
        Integer.MAX_VALUE,
        0.7f,
        1024,
        true
    );
    paged = new PagedAggregationHashTable<>(
        pagedLease,
        GrouperTestUtil.intKeySerde(),
        aggregators(pagedSelectors),
        Integer.MAX_VALUE,
        0.7f,
        1024
    );
    adaptive = new AdaptivePagedAggregationHashTable<>(
        adaptiveLease,
        GrouperTestUtil.intKeySerde(),
        aggregators(adaptiveSelectors),
        Integer.MAX_VALUE,
        0.7f,
        1024
    );
    legacy.init();
    paged.init();
    adaptive.init();
    distinctKeys = new IntKey[cardinality];
    for (int i = 0; i < cardinality; i++) {
      distinctKeys[i] = new IntKey(i);
    }
    updateKeys = new IntKey[ROW_COUNT];
    final SplittableRandom random = new SplittableRandom(0xD1_71_D5L);
    final double[] zipfCumulativeWeights = "zipf".equals(distribution) ? zipfCumulativeWeights() : null;
    for (int i = 0; i < ROW_COUNT; i++) {
      updateKeys[i] = new IntKey(keyForRow(i, random, zipfCumulativeWeights));
    }
  }

  @Setup(Level.Iteration)
  public void prepareUpdateOnlyTables()
  {
    aggregateLegacy(distinctKeys, true);
    aggregatePaged(distinctKeys, true);
    aggregateAdaptive(distinctKeys, true);
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    legacy.close();
    paged.close();
    adaptive.close();
    pagedLease.close();
    adaptiveLease.close();
    ByteBufferUtils.free(legacyBuffer);
  }

  @Benchmark
  public int legacyBufferHashGrouper()
  {
    return aggregateLegacy(updateKeys, true);
  }

  @Benchmark
  public int pagedAggregationHashTable()
  {
    return aggregatePaged(updateKeys, true);
  }

  @Benchmark
  public int adaptivePagedAggregationHashTable()
  {
    return aggregateAdaptive(updateKeys, true);
  }

  @Benchmark
  public int legacyUpdateOnly()
  {
    return aggregateLegacy(updateKeys, false);
  }

  @Benchmark
  public int pagedUpdateOnly()
  {
    return aggregatePaged(updateKeys, false);
  }

  @Benchmark
  public int adaptiveUpdateOnly()
  {
    return aggregateAdaptive(updateKeys, false);
  }

  @Benchmark
  public int legacyBuildOnly()
  {
    return aggregateLegacy(distinctKeys, true);
  }

  @Benchmark
  public int pagedBuildOnly()
  {
    return aggregatePaged(distinctKeys, true);
  }

  @Benchmark
  public int adaptiveBuildOnly()
  {
    return aggregateAdaptive(distinctKeys, true);
  }

  private int aggregateLegacy(final IntKey[] inputKeys, final boolean reset)
  {
    if (reset) {
      legacy.reset();
    }
    int count = 0;
    for (final IntKey key : inputKeys) {
      if (!legacy.aggregate(key).isOk()) {
        throw new IllegalStateException("Benchmark table filled before all rows were aggregated");
      }
      count++;
    }
    return count;
  }

  private int aggregatePaged(final IntKey[] inputKeys, final boolean reset)
  {
    if (reset) {
      paged.reset();
    }
    int count = 0;
    for (final IntKey key : inputKeys) {
      if (!paged.aggregate(key).isOk()) {
        throw new IllegalStateException("Benchmark table filled before all rows were aggregated");
      }
      count++;
    }
    return count;
  }

  private int aggregateAdaptive(final IntKey[] inputKeys, final boolean reset)
  {
    if (reset) {
      adaptive.reset();
    }
    int count = 0;
    for (final IntKey key : inputKeys) {
      if (!adaptive.aggregate(key).isOk()) {
        throw new IllegalStateException("Benchmark table filled before all rows were aggregated");
      }
      count++;
    }
    return count;
  }

  private int keyForRow(
      final int row,
      final SplittableRandom random,
      final double[] zipfCumulativeWeights
  )
  {
    switch (distribution) {
      case "cyclic":
        return row % cardinality;
      case "uniform":
        return random.nextInt(cardinality);
      case "zipf":
        return sampleZipf(random.nextDouble(), zipfCumulativeWeights);
      default:
        throw new IllegalStateException("Unknown distribution " + distribution);
    }
  }

  private double[] zipfCumulativeWeights()
  {
    final double[] cumulativeWeights = new double[cardinality];
    double totalWeight = 0;
    for (int i = 1; i <= cardinality; i++) {
      totalWeight += 1.0 / Math.pow(i, ZIPF_EXPONENT);
      cumulativeWeights[i - 1] = totalWeight;
    }
    for (int i = 0; i < cardinality; i++) {
      cumulativeWeights[i] /= totalWeight;
    }
    return cumulativeWeights;
  }

  private static int sampleZipf(final double value, final double[] cumulativeWeights)
  {
    int low = 0;
    int high = cumulativeWeights.length - 1;
    while (low < high) {
      final int middle = (low + high) >>> 1;
      if (value <= cumulativeWeights[middle]) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    return low;
  }

  private AggregatorAdapters aggregators(final GroupByTestColumnSelectorFactory selectors)
  {
    if ("count".equals(aggregation)) {
      return AggregatorAdapters.factorizeBuffered(
          selectors,
          ImmutableList.of(new CountAggregatorFactory("count"))
      );
    }
    if ("sum".equals(aggregation)) {
      return AggregatorAdapters.factorizeBuffered(
          selectors,
          ImmutableList.of(new LongSumAggregatorFactory("valueSum", "value"))
      );
    }
    if ("countSum".equals(aggregation)) {
      return AggregatorAdapters.factorizeBuffered(
          selectors,
          ImmutableList.of(new CountAggregatorFactory("count"), new LongSumAggregatorFactory("valueSum", "value"))
      );
    }
    if (!"sumCount".equals(aggregation)) {
      throw new IllegalStateException("Unknown aggregation " + aggregation);
    }
    return AggregatorAdapters.factorizeBuffered(
        selectors,
        ImmutableList.of(new LongSumAggregatorFactory("valueSum", "value"), new CountAggregatorFactory("count"))
    );
  }
}
