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
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafkainput.KafkaInputFormat;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Measures one real Kafka value read through {@link KafkaInputFormat}.
 *
 * <p>Each invocation installs a fresh {@link KafkaRecordEntity} wrapper in the reusable source before calling
 * {@link InputEntityReader#read()}.  The value parser, Kafka metadata blending, row construction, and selected row
 * field reads therefore remain on the measured path.  The runner selects a bounded set of cases combining compact and
 * wide messages, explicit versus discovery dimensions, and regular, one-time, or repeated Kafka metadata reads
 * without introducing key or header parsing.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class KafkaInputFormatReadBenchmark
{
  private static final int COMPACT_MESSAGE_BYTES = 256;
  private static final int WIDE_MESSAGE_BYTES = 4096;
  private static final int COMPACT_FIELD_COUNT = 6;
  private static final int WIDE_FIELD_COUNT = 70;
  private static final long RECORD_TIMESTAMP = 1_700_000_000_123L;
  private static final long RECORD_OFFSET = 9_876_543_210L;
  private static final int RECORD_PARTITION = 7;
  private static final long VALUE_TIMESTAMP = 1_624_579_200_000L;
  private static final long ALTERNATE_VALUE_TIMESTAMP = VALUE_TIMESTAMP + 1;
  private static final String RECORD_TOPIC = "kafka-benchmark-topic";
  private static final String VALUE_TIMESTAMP_TEXT = "2021-06-25T00:00:00.000Z";
  private static final String ALTERNATE_VALUE_TIMESTAMP_TEXT = "2021-06-25T00:00:00.001Z";
  private static final String METADATA_TIMESTAMP_FIELD = "kafka.timestamp";
  private static final String METADATA_TOPIC_FIELD = "kafka.topic";
  private static final String METADATA_PARTITION_FIELD = "kafka.partition";
  private static final String METADATA_OFFSET_FIELD = "kafka.offset";
  private static final List<String> EXPLICIT_DIMENSIONS = ImmutableList.of(
      "field00",
      "field01",
      "field02",
      "field03",
      "field04",
      "field05"
  );
  private static final JsonInputFormat VALUE_FORMAT = new JsonInputFormat(
      null,
      null,
      null,
      false,
      false,
      false
  );

  @Param({"256", "4096"})
  private int messageSize;

  @Param({"discovery", "explicit"})
  private String dimensionMode;

  @Param({"regular", "metadata", "metadataRepeated"})
  private String accessMode;

  private SettableByteEntity<KafkaRecordEntity> source;
  private InputEntityReader reader;
  private ConsumerRecord<byte[], byte[]> record;
  private ConsumerRecord<byte[], byte[]> alternateRecord;
  private List<String> regularFields;
  private int expectedDimensionCount;
  private int recordIndex;

  @Param({"constant", "alternating"})
  private String timestampPattern;

  @Setup
  public void setUp()
  {
    final int regularFieldCount;
    if (messageSize == COMPACT_MESSAGE_BYTES) {
      regularFieldCount = COMPACT_FIELD_COUNT;
    } else if (messageSize == WIDE_MESSAGE_BYTES) {
      regularFieldCount = WIDE_FIELD_COUNT;
    } else {
      throw new IllegalStateException("Unexpected benchmark message size: " + messageSize);
    }

    regularFields = fieldNames(regularFieldCount);
    record = createRecord(createValueBytes(messageSize, regularFields, VALUE_TIMESTAMP_TEXT));
    alternateRecord = createRecord(createValueBytes(messageSize, regularFields, ALTERNATE_VALUE_TIMESTAMP_TEXT));
    recordIndex = 0;
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
    expectedDimensionCount = "discovery".equals(dimensionMode)
                            ? regularFieldCount + 4
                            : EXPLICIT_DIMENSIONS.size();
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
    validateSetupRead(record, VALUE_TIMESTAMP);
    if ("alternating".equals(timestampPattern)) {
      validateSetupRead(alternateRecord, ALTERNATE_VALUE_TIMESTAMP);
    }
  }

  @Benchmark
  public void parseAndRead(final Blackhole blackhole) throws IOException
  {
    // Keep the wrapper creation on the hot path: this is the object shape used by Kafka ingestion for every record.
    final boolean useAlternate = "alternating".equals(timestampPattern) && (recordIndex++ & 1) != 0;
    final ConsumerRecord<byte[], byte[]> currentRecord = useAlternate ? alternateRecord : record;
    final long expectedTimestamp = useAlternate ? ALTERNATE_VALUE_TIMESTAMP : VALUE_TIMESTAMP;
    source.setEntity(new KafkaRecordEntity(currentRecord));
    int rowCount = 0;
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      while (rows.hasNext()) {
        final InputRow row = rows.next();
        rowCount++;
        consumeRow(row, expectedTimestamp, blackhole);
      }
    }
    if (rowCount != 1) {
      throw new IllegalStateException("Expected one row, got " + rowCount);
    }
  }

  private void consumeRow(final InputRow row, final long expectedTimestamp, final Blackhole blackhole)
  {
    if (row.getTimestampFromEpoch() != expectedTimestamp) {
      throw new IllegalStateException("Unexpected value timestamp: " + row.getTimestampFromEpoch());
    }
    if (row.getDimensions().size() != expectedDimensionCount) {
      throw new IllegalStateException(
          "Unexpected dimension count: " + row.getDimensions().size() + ", expected " + expectedDimensionCount
      );
    }

    final Object timestamp = row.getRaw("timestamp");
    if (timestamp == null) {
      throw new IllegalStateException("Value row is missing its timestamp field");
    }
    blackhole.consume(timestamp);
    final List<String> fieldsToRead = "discovery".equals(dimensionMode) ? regularFields : EXPLICIT_DIMENSIONS;
    for (final String field : fieldsToRead) {
      final Object value = row.getRaw(field);
      if (value == null) {
        throw new IllegalStateException("Value row is missing field: " + field);
      }
      blackhole.consume(value);
    }
    blackhole.consume(row.getDimensions().size());

    if ("metadata".equals(accessMode) || "metadataRepeated".equals(accessMode)) {
      final int reads = "metadataRepeated".equals(accessMode) ? 4 : 1;
      for (int i = 0; i < reads; i++) {
        // The timestamp and offset deliberately sit outside common boxed caches.
        final Object metadataTimestamp = row.getRaw(METADATA_TIMESTAMP_FIELD);
        final Object metadataTopic = row.getRaw(METADATA_TOPIC_FIELD);
        final Object metadataPartition = row.getRaw(METADATA_PARTITION_FIELD);
        final Object metadataOffset = row.getRaw(METADATA_OFFSET_FIELD);
        if (!(metadataTimestamp instanceof Long)
            || !RECORD_TOPIC.equals(metadataTopic)
            || !(metadataPartition instanceof Integer)
            || !(metadataOffset instanceof Long)) {
          throw new IllegalStateException("Kafka metadata has unexpected raw types or values");
        }
        blackhole.consume(metadataTimestamp);
        blackhole.consume(metadataTopic);
        blackhole.consume(metadataPartition);
        blackhole.consume(metadataOffset);
      }
    }
  }

  private void validateSetupRead(
      final ConsumerRecord<byte[], byte[]> recordToValidate,
      final long expectedTimestamp
  )
  {
    source.setEntity(new KafkaRecordEntity(recordToValidate));
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      if (!rows.hasNext()) {
        throw new IllegalStateException("Reader returned no rows during setup validation");
      }
      final InputRow row = rows.next();
      if (rows.hasNext()) {
        throw new IllegalStateException("Reader returned multiple rows during setup validation");
      }
      if (row.getTimestampFromEpoch() != expectedTimestamp || row.getDimensions().size() != expectedDimensionCount) {
        throw new IllegalStateException("Value row has unexpected timestamp or dimensions during setup validation");
      }
      if (row.getRaw("timestamp") == null) {
        throw new IllegalStateException("Value row is missing its timestamp field during setup validation");
      }
      final List<String> fieldsToCheck = "discovery".equals(dimensionMode) ? regularFields : EXPLICIT_DIMENSIONS;
      for (final String field : fieldsToCheck) {
        if (row.getRaw(field) == null) {
          throw new IllegalStateException("Value row is missing field during setup validation: " + field);
        }
      }
      final Object metadataTimestamp = row.getRaw(METADATA_TIMESTAMP_FIELD);
      final Object metadataTopic = row.getRaw(METADATA_TOPIC_FIELD);
      final Object metadataPartition = row.getRaw(METADATA_PARTITION_FIELD);
      final Object metadataOffset = row.getRaw(METADATA_OFFSET_FIELD);
      if (!Long.valueOf(RECORD_TIMESTAMP).equals(metadataTimestamp)
          || !RECORD_TOPIC.equals(metadataTopic)
          || !Integer.valueOf(RECORD_PARTITION).equals(metadataPartition)
          || !Long.valueOf(RECORD_OFFSET).equals(metadataOffset)) {
        throw new IllegalStateException("Kafka metadata has unexpected values during setup validation");
      }
    }
    catch (IOException e) {
      throw new IllegalStateException("Reader failed during setup validation", e);
    }
  }

  private static List<String> fieldNames(final int count)
  {
    final List<String> fields = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      fields.add(String.format("field%02d", i));
    }
    return Collections.unmodifiableList(fields);
  }

  private static ConsumerRecord<byte[], byte[]> createRecord(final byte[] valueBytes)
  {
    return new ConsumerRecord<>(
        RECORD_TOPIC,
        RECORD_PARTITION,
        RECORD_OFFSET,
        RECORD_TIMESTAMP,
        TimestampType.CREATE_TIME,
        -1,
        valueBytes.length,
        null,
        valueBytes,
        new RecordHeaders(),
        Optional.empty()
    );
  }

  private static byte[] createValueBytes(
      final int targetBytes,
      final List<String> fields,
      final String timestampText
  )
  {
    final String lastField = fields.get(fields.size() - 1);
    final StringBuilder prefix = new StringBuilder(targetBytes);
    prefix.append("{\"timestamp\":\"").append(timestampText).append("\"");
    for (int i = 0; i < fields.size() - 1; i++) {
      final String field = fields.get(i);
      prefix.append(",\"").append(field).append("\":\"value").append(String.format("%02d", i)).append("\"");
    }
    prefix.append(",\"").append(lastField).append("\":\"");
    final String suffix = "\"}";
    final int paddingLength = targetBytes - prefix.length() - suffix.length();
    if (paddingLength < 1) {
      throw new IllegalStateException("Benchmark value does not fit target size: " + targetBytes);
    }
    prefix.append("x".repeat(paddingLength)).append(suffix);
    final byte[] valueBytes = prefix.toString().getBytes(StandardCharsets.UTF_8);
    if (valueBytes.length != targetBytes) {
      throw new IllegalStateException("Benchmark value has unexpected size: " + valueBytes.length);
    }
    return valueBytes;
  }

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(KafkaInputFormatReadBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }
}
