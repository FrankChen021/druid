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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the presence bookkeeping in the real {@link IncrementalIndex#add(InputRow)} path.
 *
 * <p>Rows are constructed once per trial and have a deliberately bounded rollup cardinality.  A fresh fixed-rollup
 * on-heap index is built in invocation setup, outside the timed method, and validated in invocation teardown.  The
 * sparse case supplies every even dimension and omits every odd dimension on every row.</p>
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
public class DimensionPresenceBenchmark
{
  public static final String SIX_DENSE = "sixDense";
  public static final String SIXTY_FOUR_DENSE = "sixtyFourDense";
  public static final String SIXTY_FOUR_SPARSE = "sixtyFourSparse";

  private static final String METRIC_NAME = "count";
  private static final long ROW_TIMESTAMP = 1_700_000_000_000L;
  private static final int ROW_COUNT = 4_096;
  private static final int VALUE_CARDINALITY = 32;
  private static final int MAX_INDEX_ROWS = VALUE_CARDINALITY * 2;
  private static final int MAX_DIMENSIONS = 64;
  private static final List<String> DIMENSION_NAMES = makeDimensionNames();

  @Param({SIX_DENSE, SIXTY_FOUR_DENSE, SIXTY_FOUR_SPARSE})
  private String scenario;

  private List<InputRow> rows;
  private IncrementalIndex index;
  private int dimensionCount;
  private boolean sparse;

  @Setup(Level.Trial)
  public void setupRows()
  {
    switch (scenario) {
      case SIX_DENSE:
        dimensionCount = 6;
        sparse = false;
        break;
      case SIXTY_FOUR_DENSE:
        dimensionCount = 64;
        sparse = false;
        break;
      case SIXTY_FOUR_SPARSE:
        dimensionCount = 64;
        sparse = true;
        break;
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }

    rows = buildRows(dimensionCount, sparse);
  }

  @Setup(Level.Invocation)
  public void setupIndex()
  {
    final List<DimensionSchema> dimensionSchemas = new ArrayList<>(dimensionCount);
    for (int i = 0; i < dimensionCount; i++) {
      dimensionSchemas.add(new StringDimensionSchema(DIMENSION_NAMES.get(i)));
    }

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(dimensionSchemas))
        .withMetrics(new CountAggregatorFactory(METRIC_NAME))
        .withRollup(true)
        .build();

    index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(MAX_INDEX_ROWS)
        .build();
  }

  @TearDown(Level.Invocation)
  public void tearDownIndex()
  {
    try {
      validateIndex();
    }
    finally {
      if (index != null) {
        index.close();
        index = null;
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(ROW_COUNT)
  public void addRows(final Blackhole blackhole)
  {
    for (InputRow row : rows) {
      final IncrementalIndexAddResult result = index.add(row);
      blackhole.consume(result.getRowCount());
      blackhole.consume(result.getBytesInMemory());
    }
  }

  private void validateIndex()
  {
    if (index == null) {
      throw new IllegalStateException("Index was not initialized");
    }
    if (index.numRows() != VALUE_CARDINALITY) {
      throw new IllegalStateException("Expected " + VALUE_CARDINALITY + " rollup rows, got " + index.numRows());
    }
    if (index.getDimensionNames(false).size() != dimensionCount) {
      throw new IllegalStateException(
          "Expected " + dimensionCount + " dimensions, got " + index.getDimensionNames(false).size()
      );
    }

    final long expectedCountPerRollupRow = ROW_COUNT / VALUE_CARDINALITY;
    long totalCount = 0;
    int observedRows = 0;
    for (Row row : index) {
      observedRows++;
      final Number count = row.getMetric(METRIC_NAME);
      if (count == null || count.longValue() != expectedCountPerRollupRow) {
        throw new IllegalStateException("Unexpected count metric: " + count);
      }

      final List<String> firstDimension = row.getDimension(DIMENSION_NAMES.get(0));
      if (firstDimension.size() != 1) {
        throw new IllegalStateException("Missing rollup dimension value: " + firstDimension);
      }
      final String expectedValue = firstDimension.get(0);
      for (int i = 0; i < dimensionCount; i++) {
        final List<String> actual = row.getDimension(DIMENSION_NAMES.get(i));
        final boolean present = !sparse || (i & 1) == 0;
        if (present) {
          if (actual.size() != 1 || !expectedValue.equals(actual.get(0))) {
            throw new IllegalStateException(
                "Unexpected value for " + DIMENSION_NAMES.get(i) + ": " + actual
            );
          }
        } else if (!actual.isEmpty()) {
          throw new IllegalStateException(
              "Omitted dimension " + DIMENSION_NAMES.get(i) + " unexpectedly had value: " + actual
          );
        }
      }
      totalCount += count.longValue();
    }

    if (observedRows != VALUE_CARDINALITY || totalCount != ROW_COUNT) {
      throw new IllegalStateException(
          "Unexpected validation totals: rows=" + observedRows + ", count=" + totalCount
      );
    }
  }

  private static List<InputRow> buildRows(final int dimensionCount, final boolean sparse)
  {
    final List<InputRow> builtRows = new ArrayList<>(ROW_COUNT);
    for (int rowNumber = 0; rowNumber < ROW_COUNT; rowNumber++) {
      final String value = "v" + (rowNumber % VALUE_CARDINALITY);
      final Map<String, Object> event = new HashMap<>();
      final List<String> rowDimensions = new ArrayList<>(dimensionCount);
      for (int dimension = 0; dimension < dimensionCount; dimension++) {
        if (!sparse || (dimension & 1) == 0) {
          final String name = DIMENSION_NAMES.get(dimension);
          rowDimensions.add(name);
          event.put(name, value);
        }
      }
      builtRows.add(
          new MapBasedInputRow(
              ROW_TIMESTAMP,
              List.copyOf(rowDimensions),
              Collections.unmodifiableMap(event)
          )
      );
    }
    return List.copyOf(builtRows);
  }

  private static List<String> makeDimensionNames()
  {
    final List<String> names = new ArrayList<>(MAX_DIMENSIONS);
    for (int i = 0; i < MAX_DIMENSIONS; i++) {
      names.add("d" + i);
    }
    return List.copyOf(names);
  }

  public static void main(final String[] args)
  {
    if (args.length == 0 || !"--validate".equals(args[0])) {
      throw new IllegalArgumentException("Use --validate [scenario]");
    }

    final List<String> scenarios;
    if (args.length > 1) {
      scenarios = List.of(args[1]);
    } else {
      scenarios = List.of(SIX_DENSE, SIXTY_FOUR_DENSE, SIXTY_FOUR_SPARSE);
    }

    for (String requestedScenario : scenarios) {
      final DimensionPresenceBenchmark state = new DimensionPresenceBenchmark();
      state.scenario = requestedScenario;
      state.setupRows();
      state.setupIndex();
      try {
        for (InputRow row : state.rows) {
          state.index.add(row);
        }
        state.validateIndex();
        System.out.println("validated " + requestedScenario);
      }
      finally {
        state.index.close();
      }
    }
  }
}
