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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
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
 * Measures the per-record key path in {@code KafkaInputFormat.createReader}.
 *
 * <p>The original path normalizes the JSON format and creates its key schema for every key.  The optimized path
 * performs those two operations once, while both paths still create a reader and deserialize one key record per
 * invocation.  The normalized fresh-reader and reusable-reader paths isolate the cost of creating a key reader for
 * every key.</p>
 */
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class KafkaInputFormatKeyDeserializationBenchmark
{
  private static final byte[] SIMPLE_KEY_BYTES = "{\"key\":\"sampleKey\"}".getBytes(StandardCharsets.UTF_8);
  private static final byte[] ALTERNATE_SIMPLE_KEY_BYTES = "{\"key\":\"otherKey\"}".getBytes(StandardCharsets.UTF_8);
  private static final byte[] NESTED_KEY_BYTES = "{\"key\":{\"nested\":\"sampleKey\"}}".getBytes(
      StandardCharsets.UTF_8
  );
  private static final byte[] ALTERNATE_NESTED_KEY_BYTES = "{\"key\":{\"nested\":\"otherKey\"}}".getBytes(
      StandardCharsets.UTF_8
  );
  private static final TimestampSpec DUMMY_TIMESTAMP_SPEC = new TimestampSpec(
      "__kif_auto_timestamp",
      "auto",
      DateTimes.EPOCH
  );
  private static final InputRowSchema KEY_INPUT_ROW_SCHEMA = new InputRowSchema(
      DUMMY_TIMESTAMP_SPEC,
      DimensionsSpec.builder().useSchemaDiscovery(true).build(),
      ColumnsFilter.all()
  );
  private static final JsonInputFormat KEY_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, ImmutableList.of()),
      null,
      null,
      false,
      false,
      false
  );
  private static final InputFormat NORMALIZED_KEY_FORMAT = JsonInputFormat.withLineSplittable(KEY_FORMAT, false);
  private final SettableByteEntity<ByteEntity> reusableKeySource = new SettableByteEntity<>();
  private final InputEntityReader reusableKeyReader = NORMALIZED_KEY_FORMAT.createReader(
      KEY_INPUT_ROW_SCHEMA,
      reusableKeySource,
      null
  );
  private int simpleKeyIndex;
  private int nestedKeyIndex;

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(KafkaInputFormatKeyDeserializationBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }

  @Benchmark
  public void originalSimple(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(
        JsonInputFormat.withLineSplittable(KEY_FORMAT, false),
        new InputRowSchema(
            DUMMY_TIMESTAMP_SPEC,
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        SIMPLE_KEY_BYTES,
        blackhole
    );
  }

  @Benchmark
  public void optimizedSimple(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(NORMALIZED_KEY_FORMAT, KEY_INPUT_ROW_SCHEMA, SIMPLE_KEY_BYTES, blackhole);
  }

  @Benchmark
  public void originalNested(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(
        JsonInputFormat.withLineSplittable(KEY_FORMAT, false),
        new InputRowSchema(
            DUMMY_TIMESTAMP_SPEC,
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        NESTED_KEY_BYTES,
        blackhole
    );
  }

  @Benchmark
  public void optimizedNested(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(NORMALIZED_KEY_FORMAT, KEY_INPUT_ROW_SCHEMA, NESTED_KEY_BYTES, blackhole);
  }

  @Benchmark
  public void normalizedSimpleFreshReader(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(
        NORMALIZED_KEY_FORMAT,
        KEY_INPUT_ROW_SCHEMA,
        nextSimpleKeyBytes(),
        blackhole
    );
  }

  @Benchmark
  public void normalizedSimpleReusableReader(final Blackhole blackhole) throws IOException
  {
    consumeWithReusableKeyReader(nextSimpleKeyBytes(), blackhole);
  }

  @Benchmark
  public void normalizedNestedFreshReader(final Blackhole blackhole) throws IOException
  {
    consumeFirstKeyValue(
        NORMALIZED_KEY_FORMAT,
        KEY_INPUT_ROW_SCHEMA,
        nextNestedKeyBytes(),
        blackhole
    );
  }

  @Benchmark
  public void normalizedNestedReusableReader(final Blackhole blackhole) throws IOException
  {
    consumeWithReusableKeyReader(nextNestedKeyBytes(), blackhole);
  }

  private static void consumeFirstKeyValue(
      final InputFormat keyFormat,
      final InputRowSchema keyInputRowSchema,
      final byte[] keyBytes,
      final Blackhole blackhole
  ) throws IOException
  {
    final InputEntityReader keyReader = keyFormat.createReader(
        keyInputRowSchema,
        new ByteEntity(keyBytes),
        null
    );
    try (final CloseableIterator<InputRow> keyIterator = keyReader.read()) {
      if (!keyIterator.hasNext()) {
        throw new IllegalStateException("Key reader returned no rows");
      }
      blackhole.consume(keyIterator.next().getRaw("key"));
    }
  }

  private void consumeWithReusableKeyReader(final byte[] keyBytes, final Blackhole blackhole) throws IOException
  {
    reusableKeySource.setEntity(new ByteEntity(keyBytes));
    consumeFirstKeyValue(reusableKeyReader, blackhole);
  }

  private byte[] nextSimpleKeyBytes()
  {
    return (simpleKeyIndex++ & 1) == 0 ? SIMPLE_KEY_BYTES : ALTERNATE_SIMPLE_KEY_BYTES;
  }

  private byte[] nextNestedKeyBytes()
  {
    return (nestedKeyIndex++ & 1) == 0 ? NESTED_KEY_BYTES : ALTERNATE_NESTED_KEY_BYTES;
  }

  private static void consumeFirstKeyValue(
      final InputEntityReader keyReader,
      final Blackhole blackhole
  ) throws IOException
  {
    try (final CloseableIterator<InputRow> keyIterator = keyReader.read()) {
      if (!keyIterator.hasNext()) {
        throw new IllegalStateException("Key reader returned no rows");
      }
      blackhole.consume(keyIterator.next().getRaw("key"));
    }
  }
}
