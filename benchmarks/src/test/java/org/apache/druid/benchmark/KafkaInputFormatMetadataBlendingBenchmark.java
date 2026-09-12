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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafkainput.KafkaInputFormat;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures Kafka metadata blending around a single JSON value record.
 *
 * <p>The Kafka record and JSON bytes are prepared once per trial.  Each operation creates only the
 * {@link KafkaRecordEntity} wrapper, installs it in the reusable source, and reads one row through the real
 * {@link KafkaInputFormat} reader.  The dimension parameter compares schema discovery with a fixed dimension list.
 * Both the normal read and sampling paths are measured because they blend Kafka metadata with value rows.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class KafkaInputFormatMetadataBlendingBenchmark
{
  private static final int TARGET_MESSAGE_BYTES = 256;
  private static final String JSON_PREFIX =
      "{\"timestamp\":\"2021-06-25T00:00:00.000Z\",\"city\":\"Singapore\",\"country\":\"SG\","
      + "\"service\":\"analytics\",\"host\":\"broker-a\",\"status\":\"ok\",\"count\":42,\"payload\":\"";
  private static final String JSON_SUFFIX = "\"}";
  private static final List<String> JSON_FIELDS = ImmutableList.of(
      "timestamp",
      "city",
      "country",
      "service",
      "host",
      "status",
      "count",
      "payload"
  );
  private static final List<String> EXPLICIT_DIMENSIONS = ImmutableList.of(
      "city",
      "country",
      "service",
      "host",
      "status",
      "payload"
  );
  private static final JsonInputFormat VALUE_FORMAT = new JsonInputFormat(
      null,
      null,
      null,
      false,
      false,
      false
  );

  @Param({"discovery", "explicit"})
  private String dimensionMode;

  private SettableByteEntity<KafkaRecordEntity> source;
  private InputEntityReader reader;
  private ConsumerRecord<byte[], byte[]> record;
  private byte[] valueBytes;

  @Setup
  public void setUp()
  {
    final int paddingLength = TARGET_MESSAGE_BYTES - JSON_PREFIX.length() - JSON_SUFFIX.length();
    if (paddingLength < 0) {
      throw new IllegalStateException("Benchmark JSON prefix is larger than the target message size");
    }
    valueBytes = (JSON_PREFIX + "x".repeat(paddingLength) + JSON_SUFFIX).getBytes(StandardCharsets.UTF_8);
    if (valueBytes.length != TARGET_MESSAGE_BYTES) {
      throw new IllegalStateException("Benchmark message has unexpected size: " + valueBytes.length);
    }

    record = new ConsumerRecord<>("kafka-benchmark", 0, 0L, null, valueBytes);
    source = new SettableByteEntity<>();
    source.setEntity(new KafkaRecordEntity(record));

    final DimensionsSpec dimensionsSpec = "discovery".equals(dimensionMode)
                                         ? DimensionsSpec.builder().useSchemaDiscovery(true).build()
                                         : new DimensionsSpec(DimensionsSpec.getDefaultSchemas(EXPLICIT_DIMENSIONS));
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    );
    reader = new KafkaInputFormat(
        null,
        null,
        VALUE_FORMAT,
        null,
        null,
        null,
        null,
        null,
        null
    ).createReader(inputRowSchema, source, null);
  }

  @Benchmark
  public void parseAndRead(final Blackhole blackhole) throws IOException
  {
    source.setEntity(new KafkaRecordEntity(record));
    int rowCount = 0;
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      while (rows.hasNext()) {
        final InputRow row = rows.next();
        rowCount++;
        consumeRow(row, blackhole);
      }
    }
    if (rowCount != 1) {
      throw new IllegalStateException("Expected one row, got " + rowCount);
    }
  }

  @Benchmark
  public void parseAndSample(final Blackhole blackhole) throws IOException
  {
    source.setEntity(new KafkaRecordEntity(record));
    int sampleCount = 0;
    try (final CloseableIterator<InputRowListPlusRawValues> samples = reader.sample()) {
      while (samples.hasNext()) {
        final InputRowListPlusRawValues sample = samples.next();
        sampleCount++;
        for (final Map<String, Object> raw : sample.getRawValuesList()) {
          consumeRawValues(raw, blackhole);
        }
        for (final InputRow row : sample.getInputRows()) {
          consumeRow(row, blackhole);
        }
      }
    }
    if (sampleCount != 1) {
      throw new IllegalStateException("Expected one sample, got " + sampleCount);
    }
  }

  private static void consumeRow(final InputRow row, final Blackhole blackhole)
  {
    blackhole.consume(row.getTimestampFromEpoch());
    for (final String field : JSON_FIELDS) {
      blackhole.consume(row.getRaw(field));
    }
    blackhole.consume(row.getDimensions().size());
  }

  private static void consumeRawValues(final Map<String, Object> raw, final Blackhole blackhole)
  {
    for (final String field : JSON_FIELDS) {
      blackhole.consume(raw.get(field));
    }
    blackhole.consume(raw.size());
  }

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(KafkaInputFormatMetadataBlendingBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }
}
