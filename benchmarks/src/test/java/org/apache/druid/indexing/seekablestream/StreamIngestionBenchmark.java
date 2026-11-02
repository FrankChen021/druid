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

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs Kafka message bytes through the same path a streaming ingestion task uses for each record: the
 * {@link StreamChunkReader} (input format, transforms and filters) followed by {@link IncrementalIndex#add}.
 * Kafka fetching and persists are excluded.
 *
 * The schema mirrors a network latency datasource: JSON messages with posix timestamps, 7 string dimensions,
 * second-granularity rollup with count and doubleSum, and an expression transform used only by the filter.
 * Messages are read from {@code dataFile}, one JSON object per line.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xms8g", "-Xmx8g"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class StreamIngestionBenchmark
{
  private static final String[] DIMENSIONS = {
      "from_idc", "to_idc", "from_platform", "to_platform", "dscp", "service", "receive_dscp"
  };

  @Param({""})
  private String dataFile;

  @Param({"true", "false"})
  private boolean allowOptimizedReader;

  private List<ByteEntity> messages;
  private StreamChunkReader<ByteEntity> reader;
  private IncrementalIndexSchema indexSchema;
  private IncrementalIndex incIndex;

  @Setup
  public void setup() throws IOException
  {
    if (dataFile.isEmpty()) {
      throw new ISE("Set -p dataFile=<path to a file with one JSON message per line>");
    }
    messages = new ArrayList<>();
    for (String line : Files.readAllLines(new File(dataFile).toPath(), StandardCharsets.UTF_8)) {
      messages.add(new ByteEntity(line.getBytes(StandardCharsets.UTF_8)));
    }

    final List<DimensionSchema> dimensions = new ArrayList<>();
    for (String dimension : DIMENSIONS) {
      dimensions.add(new StringDimensionSchema(dimension));
    }
    final DimensionsSpec dimensionsSpec = DimensionsSpec.builder()
                                                        .setDimensions(dimensions)
                                                        .setDimensionExclusions(
                                                            List.of("__time", "rtt", "count", "time", "sum_rtt")
                                                        )
                                                        .build();
    final TimestampSpec timestampSpec = new TimestampSpec("time", "posix", null);
    final AggregatorFactory[] metrics = {
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("sum_rtt", "rtt")
    };
    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            new NotDimFilter(new SelectorDimFilter("from_idc", null, null)),
            new NotDimFilter(new SelectorDimFilter("rtt", "0", null)),
            new NotDimFilter(new SelectorDimFilter("is_drop", "1", null))
        ),
        List.of(new ExpressionTransform("is_drop", "CAST(CAST(rtt > 1000, 'LONG'), 'STRING')", ExprMacroTable.nil()))
    );
    final DataSchema dataSchema =
        DataSchema.builder()
                  .withDataSource("benchmark")
                  .withTimestamp(timestampSpec)
                  .withDimensions(dimensionsSpec)
                  .withAggregators(metrics)
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, Granularities.SECOND, true, null))
                  .withTransform(transformSpec)
                  .build();
    final InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(dataSchema);
    final JsonInputFormat inputFormat = new JsonInputFormat(
        new JSONPathSpec(true, Collections.emptyList()),
        null,
        true,
        false,
        false,
        false,
        allowOptimizedReader
    );

    final SimpleRowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    reader = new StreamChunkReader<>(
        inputFormat,
        inputRowSchema,
        transformSpec,
        null,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0)
    );

    indexSchema = IncrementalIndexSchema.builder()
                                        .withTimestampSpec(timestampSpec)
                                        .withQueryGranularity(Granularities.SECOND)
                                        .withDimensionsSpec(dimensionsSpec)
                                        .withMetrics(metrics)
                                        .withRollup(true)
                                        .build();
  }

  @Setup(Level.Invocation)
  public void setupIndex()
  {
    incIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(messages.size() * 2)
        .build();
  }

  @TearDown(Level.Invocation)
  public void tearDown()
  {
    if (incIndex != null) {
      incIndex.close();
    }
  }

  /**
   * Input format, transforms and filters only.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parse(Blackhole blackhole) throws IOException
  {
    for (ByteEntity message : messages) {
      blackhole.consume(reader.parse(Collections.singletonList(message), false));
    }
  }

  /**
   * Parsing plus adding each row to a rollup incremental index, as the ingestion thread does.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseAndIndex(Blackhole blackhole) throws IOException
  {
    for (ByteEntity message : messages) {
      for (InputRow row : reader.parse(Collections.singletonList(message), false)) {
        blackhole.consume(incIndex.add(row).getRowCount());
      }
    }
  }
}
