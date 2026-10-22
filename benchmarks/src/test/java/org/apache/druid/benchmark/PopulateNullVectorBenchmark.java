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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Compares the original row-at-a-time implementation of {@link VectorSelectorUtils#populateNullVector} ("old", copied
 * verbatim into {@link #oldPopulateNullVector}) against the current one ("new"). Each invocation scans a whole column
 * in vector-sized steps with a single shared null iterator, the way the numeric column vector selectors do.
 *
 * <p>Build and run (from the repository root):
 * <pre>
 * mvn package -pl benchmarks -am -DskipTests -Dmaven.install.skip=true \
 *   -Pskip-static-checks -Dweb.console.skip=true -T1C
 * java -jar benchmarks/target/benchmarks.jar PopulateNullVectorBenchmark
 * </pre>
 *
 * <p>Quick run with fewer params, e.g.:
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar PopulateNullVectorBenchmark -f 1 -wi 2 -i 3 -w 2s -r 2s \
 *   -p bitmapType=roaring -p nullDensity=0.01,0.5
 * </pre>
 *
 * <p>Results for "old" and "new" appear on adjacent rows of the JMH table (the "version" param varies fastest).
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PopulateNullVectorBenchmark
{
  private static final int VECTOR_SIZE = 512;

  @Param({"roaring", "concise"})
  private String bitmapType;

  /**
   * "contiguous" scans every row; "filteredN" scans a random N% of rows, as a filtered (non-contiguous) offset would.
   */
  @Param({"contiguous", "filtered50", "filtered10"})
  private String offsetType;

  @Param({"0.001", "0.01", "0.1", "0.5", "0.9"})
  private double nullDensity;

  @Param({"1000000"})
  private int numRows;

  /**
   * Implementation to run. Named so that it sorts last: JMH orders params alphabetically and varies the last one
   * fastest, which puts "old" and "new" on adjacent rows.
   */
  @Param({"old", "new"})
  private String version;

  private ImmutableBitmap nullBitmap;
  private boolean useNew;
  private boolean contiguous;

  /**
   * For non-contiguous scans: the selected rows, split into vectors of at most {@link #VECTOR_SIZE}.
   */
  private int[][] filteredVectors;
  private int[] filteredVectorSizes;

  private BenchmarkVectorOffset offset;
  private boolean[] nullVector;

  @Setup
  public void setup()
  {
    final Random random = new Random(1234);
    final BitmapFactory bitmapFactory;
    switch (bitmapType) {
      case "roaring":
        bitmapFactory = new RoaringBitmapFactory();
        break;
      case "concise":
        bitmapFactory = new ConciseBitmapFactory();
        break;
      default:
        throw DruidException.defensive("Unknown bitmapType[%s]", bitmapType);
    }

    final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
    for (int i = 0; i < numRows; i++) {
      if (random.nextDouble() < nullDensity) {
        mutableBitmap.add(i);
      }
    }
    nullBitmap = bitmapFactory.makeImmutableBitmap(mutableBitmap);

    switch (version) {
      case "old":
        useNew = false;
        break;
      case "new":
        useNew = true;
        break;

      default:
        throw DruidException.defensive("Unknown version[%s]", version);
    }

    if ("contiguous".equals(offsetType)) {
      contiguous = true;
    } else if (offsetType.startsWith("filtered")) {
      contiguous = false;
      final double selectivity = Integer.parseInt(offsetType.substring("filtered".length())) / 100.0;
      final int[] selected = IntStream.range(0, numRows).filter(i -> random.nextDouble() < selectivity).toArray();
      final int numVectors = (selected.length + VECTOR_SIZE - 1) / VECTOR_SIZE;
      filteredVectors = new int[numVectors][];
      filteredVectorSizes = new int[numVectors];
      for (int v = 0; v < numVectors; v++) {
        final int from = v * VECTOR_SIZE;
        final int to = Math.min(from + VECTOR_SIZE, selected.length);
        filteredVectors[v] = Arrays.copyOf(Arrays.copyOfRange(selected, from, to), VECTOR_SIZE);
        filteredVectorSizes[v] = to - from;
      }
    } else {
      throw DruidException.defensive("Unknown offsetType[%s]", offsetType);
    }

    offset = new BenchmarkVectorOffset(contiguous);
    nullVector = new boolean[VECTOR_SIZE];
  }

  @Benchmark
  public void scanColumn(final Blackhole blackhole)
  {
    final PeekableIntIterator nullIterator = nullBitmap.peekableIterator();
    if (contiguous) {
      for (int start = 0; start < numRows; start += VECTOR_SIZE) {
        offset.set(start, Math.min(VECTOR_SIZE, numRows - start), null);
        blackhole.consume(populate(nullIterator));
      }
    } else {
      for (int v = 0; v < filteredVectors.length; v++) {
        offset.set(0, filteredVectorSizes[v], filteredVectors[v]);
        blackhole.consume(populate(nullIterator));
      }
    }
  }

  @Nullable
  private boolean[] populate(final PeekableIntIterator nullIterator)
  {
    if (useNew) {
      return VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);
    } else {
      return oldPopulateNullVector(nullVector, offset, nullIterator);
    }
  }

  /**
   * Original implementation of {@link VectorSelectorUtils#populateNullVector}, kept as the baseline.
   */
  @Nullable
  public static boolean[] oldPopulateNullVector(
      @Nullable final boolean[] nullVector,
      final ReadableVectorOffset offset,
      final PeekableIntIterator nullIterator
  )
  {
    if (!nullIterator.hasNext()) {
      return null;
    }

    final boolean[] retVal;

    if (nullVector != null) {
      retVal = nullVector;
    } else {
      retVal = new boolean[offset.getMaxVectorSize()];
    }

    if (offset.isContiguous()) {
      final int startOffset = offset.getStartOffset();
      nullIterator.advanceIfNeeded(startOffset);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = i + startOffset;
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    } else {
      final int[] currentOffsets = offset.getOffsets();
      nullIterator.advanceIfNeeded(currentOffsets[0]);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = currentOffsets[i];
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    }

    return retVal;
  }

  /**
   * Minimal mutable offset, so that the benchmark measures null-vector construction rather than offset iteration.
   */
  private static class BenchmarkVectorOffset implements ReadableVectorOffset
  {
    private final boolean contiguous;
    private int startOffset;
    private int size;
    @Nullable
    private int[] offsets;

    BenchmarkVectorOffset(final boolean contiguous)
    {
      this.contiguous = contiguous;
    }

    void set(final int startOffset, final int size, @Nullable final int[] offsets)
    {
      this.startOffset = startOffset;
      this.size = size;
      this.offsets = offsets;
    }

    @Override
    public int getId()
    {
      return contiguous ? startOffset : offsets[0];
    }

    @Override
    public boolean isContiguous()
    {
      return contiguous;
    }

    @Override
    public int getMaxVectorSize()
    {
      return VECTOR_SIZE;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return size;
    }

    @Override
    public int getStartOffset()
    {
      return startOffset;
    }

    @Override
    public int[] getOffsets()
    {
      return offsets;
    }
  }
}
