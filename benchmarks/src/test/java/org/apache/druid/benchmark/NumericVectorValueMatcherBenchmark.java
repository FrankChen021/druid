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

import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import javax.annotation.Nullable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the scalar and SIMD ({@code druid.expressions.useVectorApi}) vectorized value matchers of
 * {@link EqualityFilter} and {@link RangeFilter} on long, double, and float columns. The {@code impl} param selects
 * the implementation, so both show up side by side in the same JMH result table. Scores are nanoseconds per vector
 * of {@code vectorSize} rows, with a mask selecting every row (the case the SIMD path handles; sparse masks fall back
 * to the scalar loop).
 *
 * Each invocation matches {@link #NUM_BATCHES} distinct pre-generated vectors, so the branch predictor cannot
 * memorize the match pattern of a single vector, which would make the branchy scalar loop look unrealistically fast.
 *
 * Build and run from the repository root (the forked JVM gets {@code --add-modules=jdk.incubator.vector} from the
 * {@link Fork} annotation):
 * <pre>
 * mvn package -pl benchmarks -am -DskipTests -Dmaven.install.skip=true \
 *   -Pskip-static-checks -Dweb.console.skip=true -T1C
 * java -jar benchmarks/target/benchmarks.jar NumericVectorValueMatcherBenchmark
 * </pre>
 * A quicker run over a subset of params, for example:
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar NumericVectorValueMatcherBenchmark -f 1 -wi 3 -i 5 -w 1s -r 1s \
 *   -p type=long,double -p filter=range -p selectivity=0.5 -p nullRatio=0
 * </pre>
 * Add {@code -rf csv -rff results.csv} to save the results.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "--add-modules=jdk.incubator.vector")
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class NumericVectorValueMatcherBenchmark
{
  private static final int NUM_BATCHES = 1024;
  private static final String COLUMN = "x";

  /**
   * Values are drawn uniformly from [0, DOMAIN).
   */
  private static final int DOMAIN = 1_000_000;

  /**
   * Equality filters match this value, which is outside the value domain, so selectivity is exact.
   */
  private static final int EQUALITY_VALUE = -1;

  @Param({"scalar", "simd"})
  private String impl;

  @Param({"long", "double", "float"})
  private String type;

  @Param({"equality", "range"})
  private String filter;

  /**
   * Fraction of non-null rows that match.
   */
  @Param({"0.01", "0.5", "0.99"})
  private double selectivity;

  @Param({"0", "0.1"})
  private double nullRatio;

  @Param({"512"})
  private int vectorSize;

  private BatchSelector selector;
  private VectorValueMatcher matcher;
  private ReadableVectorMatch mask;

  @Setup(Level.Trial)
  public void setup()
  {
    switch (impl) {
      case "scalar" -> ExpressionProcessing.initializeForTests();
      case "simd" -> ExpressionProcessing.initializeForVectorApiTests();
      default -> throw new IllegalArgumentException("Unknown impl[" + impl + "]");
    }

    final ColumnType columnType = switch (type) {
      case "long" -> ColumnType.LONG;
      case "double" -> ColumnType.DOUBLE;
      case "float" -> ColumnType.FLOAT;
      default -> throw new IllegalArgumentException("Unknown type[" + type + "]");
    };
    final boolean equality = switch (filter) {
      case "equality" -> true;
      case "range" -> false;
      default -> throw new IllegalArgumentException("Unknown filter[" + filter + "]");
    };

    final Random random = new Random(0xC0FFEEL);
    final long[][] longs = new long[NUM_BATCHES][];
    final double[][] doubles = new double[NUM_BATCHES][];
    final float[][] floats = new float[NUM_BATCHES][];
    final boolean[][] nulls = nullRatio > 0 ? new boolean[NUM_BATCHES][] : null;
    for (int b = 0; b < NUM_BATCHES; b++) {
      final double[] values = new double[vectorSize];
      for (int i = 0; i < vectorSize; i++) {
        if (equality) {
          values[i] = random.nextDouble() < selectivity ? EQUALITY_VALUE : random.nextInt(DOMAIN);
        } else {
          // integral values, so long, float, and double columns all have the same selectivity
          values[i] = random.nextInt(DOMAIN);
        }
      }
      longs[b] = new long[vectorSize];
      doubles[b] = new double[vectorSize];
      floats[b] = new float[vectorSize];
      for (int i = 0; i < vectorSize; i++) {
        longs[b][i] = (long) values[i];
        doubles[b][i] = values[i];
        floats[b][i] = (float) values[i];
      }
      if (nulls != null) {
        nulls[b] = new boolean[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
          nulls[b][i] = random.nextDouble() < nullRatio;
        }
      }
    }

    final Filter theFilter;
    if (equality) {
      theFilter = new EqualityFilter(COLUMN, ColumnType.LONG, (long) EQUALITY_VALUE, null);
    } else {
      theFilter = new RangeFilter(COLUMN, ColumnType.LONG, 0L, (long) (selectivity * DOMAIN), false, true, null);
    }

    selector = new BatchSelector(vectorSize, longs, doubles, floats, nulls);
    matcher = theFilter.makeVectorMatcher(new BatchSelectorFactory(selector, columnType));
    mask = VectorMatch.allTrue(vectorSize);
  }

  @Benchmark
  @OperationsPerInvocation(NUM_BATCHES)
  public int match()
  {
    int matched = 0;
    for (int b = 0; b < NUM_BATCHES; b++) {
      selector.setBatch(b);
      matched += matcher.match(mask, false).getSelectionSize();
    }
    return matched;
  }

  /**
   * Serves one of several pre-generated vectors, chosen by {@link #setBatch}.
   */
  private static final class BatchSelector implements VectorValueSelector, ReadableVectorInspector
  {
    private final int vectorSize;
    private final long[][] longs;
    private final double[][] doubles;
    private final float[][] floats;
    @Nullable
    private final boolean[][] nulls;
    private int batch;

    private BatchSelector(
        final int vectorSize,
        final long[][] longs,
        final double[][] doubles,
        final float[][] floats,
        @Nullable final boolean[][] nulls
    )
    {
      this.vectorSize = vectorSize;
      this.longs = longs;
      this.doubles = doubles;
      this.floats = floats;
      this.nulls = nulls;
    }

    private void setBatch(final int batch)
    {
      this.batch = batch;
    }

    @Override
    public long[] getLongVector()
    {
      return longs[batch];
    }

    @Override
    public float[] getFloatVector()
    {
      return floats[batch];
    }

    @Override
    public double[] getDoubleVector()
    {
      return doubles[batch];
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return nulls == null ? null : nulls[batch];
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getId()
    {
      return batch;
    }
  }

  private static final class BatchSelectorFactory implements VectorColumnSelectorFactory
  {
    private final BatchSelector selector;
    private final ColumnType columnType;

    private BatchSelectorFactory(final BatchSelector selector, final ColumnType columnType)
    {
      this.selector = selector;
      this.columnType = columnType;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return selector;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorValueSelector makeValueSelector(final String column)
    {
      return selector;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(final String column)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(columnType);
    }
  }
}
