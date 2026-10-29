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

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Measures one-message streaming JSON parsing with a reader reused between records.
 *
 * <p>Each operation installs one new {@link ByteEntity} in a {@link SettableByteEntity}, reads one row through the
 * normal {@link JsonInputFormat} reader, and consumes every discovered field.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class JsonInputFormatStreamingBenchmark
{
  private static final String JSON_PREFIX =
      "{\"timestamp\":\"1970-01-01T00:00:00.000Z\",\"id\":\"streaming-id\",\"value\":42,"
      + "\"nested\":{\"label\":\"streaming-label\"},\"payload\":\"";
  private static final String JSON_SUFFIX = "\"}";
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(
      "__kif_auto_timestamp",
      "auto",
      DateTimes.EPOCH
  );
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      TIMESTAMP_SPEC,
      DimensionsSpec.builder().useSchemaDiscovery(true).build(),
      ColumnsFilter.all()
  );
  private static final JsonInputFormat INPUT_FORMAT = new JsonInputFormat(
      null,
      null,
      null,
      false,
      false,
      false
  );

  @Param({"256", "4096"})
  private int payloadSize;

  private SettableByteEntity<ByteEntity> source;
  private InputEntityReader reader;
  private byte[] messageBytes;

  @Setup
  public void setUp()
  {
    final int paddingLength = payloadSize - JSON_PREFIX.length() - JSON_SUFFIX.length();
    if (paddingLength < 0) {
      throw new IllegalArgumentException("Payload size is too small");
    }

    final String message = JSON_PREFIX + "x".repeat(paddingLength) + JSON_SUFFIX;
    messageBytes = message.getBytes(StandardCharsets.UTF_8);
    source = new SettableByteEntity<>();
    reader = INPUT_FORMAT.createReader(INPUT_ROW_SCHEMA, source, null);
  }

  @Benchmark
  public void parseAndRead(final Blackhole blackhole) throws IOException
  {
    source.setEntity(new ByteEntity(messageBytes));
    int rowCount = 0;
    try (final CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        rowCount++;
        blackhole.consume(row.getTimestampFromEpoch());
        for (final String dimension : row.getDimensions()) {
          blackhole.consume(dimension);
          blackhole.consume(row.getRaw(dimension));
        }
      }
    }

    if (rowCount != 1) {
      throw new IllegalStateException("Expected one row, got " + rowCount);
    }
  }

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(JsonInputFormatStreamingBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }
}
