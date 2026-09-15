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
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures fixed-schema JSON decoding through the real {@link IncrementalIndex#add(InputRow)} path.
 *
 * <p>Each invocation parses 4,096 prebuilt records through one reusable entity and one reusable reader, setting one
 * record on the entity before each {@code read()} call, and adds every parsed row to a fresh bounded on-heap index.
 * Payload construction, reader construction, index construction, and validation are outside the timed method.  The
 * compact payload has eight selected root fields and one unused padding field; the wide payload has 70 root fields,
 * of which only six dimensions, the timestamp, and one metric are selected by the input schema.</p>
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
public class SchemaDirectedJsonBenchmark
{
  public static final int COMPACT_PAYLOAD_BYTES = 256;
  public static final int WIDE_PAYLOAD_BYTES = 4_096;
  public static final int ROW_COUNT = 4_096;

  private static final int VALUE_CARDINALITY = 32;
  private static final int TIMESTAMP_VARIANTS = 2;
  private static final int ROLLUP_ROW_COUNT = VALUE_CARDINALITY * TIMESTAMP_VARIANTS;
  private static final int WIDE_FIELD_COUNT = 70;
  private static final int PADDING_FIELD_COUNT = WIDE_FIELD_COUNT - 8;
  private static final int ROLLUP_ROWS_PER_KEY = ROW_COUNT / ROLLUP_ROW_COUNT;
  private static final long TIMESTAMP_A = 1_700_000_000_000L;
  private static final long TIMESTAMP_B = TIMESTAMP_A + 1_000L;
  private static final String TIMESTAMP_A_TEXT = "2023-11-14T22:13:20.000Z";
  private static final String TIMESTAMP_B_TEXT = "2023-11-14T22:13:21.000Z";

  private static final String TIMESTAMP_COLUMN = "ts";
  private static final String STRING_DIMENSION_0 = "s0";
  private static final String LONG_DIMENSION_0 = "l0";
  private static final String DOUBLE_DIMENSION_0 = "d0";
  private static final String STRING_DIMENSION_1 = "s1";
  private static final String LONG_DIMENSION_1 = "l1";
  private static final String DOUBLE_DIMENSION_1 = "d1";
  private static final String AMOUNT_METRIC = "amount";
  private static final String COUNT_METRIC = "count";

  private static final List<String> DIMENSION_NAMES = List.of(
      STRING_DIMENSION_0,
      LONG_DIMENSION_0,
      DOUBLE_DIMENSION_0,
      STRING_DIMENSION_1,
      LONG_DIMENSION_1,
      DOUBLE_DIMENSION_1
  );

  private static final List<DimensionSchema> DIMENSION_SCHEMAS = List.of(
      new StringDimensionSchema(STRING_DIMENSION_0),
      new LongDimensionSchema(LONG_DIMENSION_0),
      new DoubleDimensionSchema(DOUBLE_DIMENSION_0),
      new StringDimensionSchema(STRING_DIMENSION_1),
      new LongDimensionSchema(LONG_DIMENSION_1),
      new DoubleDimensionSchema(DOUBLE_DIMENSION_1)
  );

  private static final Set<String> REQUIRED_COLUMNS = Set.of(
      TIMESTAMP_COLUMN,
      STRING_DIMENSION_0,
      LONG_DIMENSION_0,
      DOUBLE_DIMENSION_0,
      STRING_DIMENSION_1,
      LONG_DIMENSION_1,
      DOUBLE_DIMENSION_1,
      AMOUNT_METRIC
  );

  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(TIMESTAMP_COLUMN, "iso", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(DIMENSION_SCHEMAS);
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      TIMESTAMP_SPEC,
      DIMENSIONS_SPEC,
      ColumnsFilter.inclusionBased(REQUIRED_COLUMNS),
      Set.of(AMOUNT_METRIC, COUNT_METRIC)
  );
  private static final JSONPathSpec FLAT_FIELD_DISCOVERY = new JSONPathSpec(true, List.of());
  private static final JsonInputFormat INPUT_FORMAT = new JsonInputFormat(
      FLAT_FIELD_DISCOVERY,
      null,
      null,
      false,
      false,
      false
  );

  @Param({"256", "4096"})
  private int payloadBytes;

  @Param({"true", "false"})
  private boolean rollup;

  private SettableByteEntity<ByteEntity> source;
  private InputEntityReader reader;
  private byte[][] payloads;
  private IncrementalIndex index;

  @Setup(Level.Trial)
  public void setupPayload()
  {
    if (payloadBytes != COMPACT_PAYLOAD_BYTES && payloadBytes != WIDE_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("Unsupported payload size: " + payloadBytes);
    }

    payloads = buildPayloads(payloadBytes);
    source = new SettableByteEntity<>();
    reader = INPUT_FORMAT.createReader(INPUT_ROW_SCHEMA, source, null);
  }

  @Setup(Level.Invocation)
  public void setupIndex()
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withDimensionsSpec(DIMENSIONS_SPEC)
        .withMetrics(
            new LongSumAggregatorFactory(AMOUNT_METRIC, AMOUNT_METRIC),
            new CountAggregatorFactory(COUNT_METRIC)
        )
        .withRollup(rollup)
        .build();

    index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(rollup ? ROLLUP_ROW_COUNT : ROW_COUNT)
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
  public void parseAndAdd(final Blackhole blackhole) throws IOException
  {
    int parsedRows = 0;
    for (int rowNumber = 0; rowNumber < ROW_COUNT; rowNumber++) {
      source.setEntity(new ByteEntity(payloads[rowNumber]));
      int rowsInMessage = 0;
      try (final CloseableIterator<InputRow> rows = reader.read()) {
        while (rows.hasNext()) {
          final InputRow row = rows.next();
          if (rowsInMessage++ != 0) {
            throw new IllegalStateException("Expected one row per input entity");
          }
          final IncrementalIndexAddResult result = index.add(row);
          parsedRows++;
          blackhole.consume(result.getRowCount());
          blackhole.consume(result.getBytesInMemory());
        }
      }
      if (rowsInMessage != 1) {
        throw new IllegalStateException("Expected one row per input entity, got " + rowsInMessage);
      }
    }

    if (parsedRows != ROW_COUNT) {
      throw new IllegalStateException("Expected " + ROW_COUNT + " parsed rows, got " + parsedRows);
    }
  }

  private void validateIndex()
  {
    if (index == null) {
      throw new IllegalStateException("Index was not initialized");
    }

    final int expectedRows = rollup ? ROLLUP_ROW_COUNT : ROW_COUNT;
    if (index.numRows() != expectedRows) {
      throw new IllegalStateException("Expected " + expectedRows + " index rows, got " + index.numRows());
    }
    if (!DIMENSION_NAMES.equals(index.getDimensionNames(false))) {
      throw new IllegalStateException("Unexpected dimensions: " + index.getDimensionNames(false));
    }
    if (!List.of(AMOUNT_METRIC, COUNT_METRIC).equals(index.getMetricNames())) {
      throw new IllegalStateException("Unexpected metrics: " + index.getMetricNames());
    }

    final Map<String, Integer> rowsPerKey = new HashMap<>();
    long totalCount = 0L;
    long totalAmount = 0L;
    int observedRows = 0;
    for (final Row row : index) {
      final int group = validateIndexedRow(row);
      final String key = row.getTimestampFromEpoch() + ":" + group;
      rowsPerKey.merge(key, 1, Integer::sum);

      final long expectedCount = rollup ? ROLLUP_ROWS_PER_KEY : 1L;
      final long expectedAmount = rollup
                                 ? ROLLUP_ROWS_PER_KEY * amountForGroup(group)
                                 : amountForGroup(group);
      final Number count = row.getMetric(COUNT_METRIC);
      final Number amount = row.getMetric(AMOUNT_METRIC);
      if (count == null || count.longValue() != expectedCount) {
        throw new IllegalStateException("Unexpected count for " + key + ": " + count);
      }
      if (amount == null || amount.longValue() != expectedAmount) {
        throw new IllegalStateException("Unexpected amount for " + key + ": " + amount);
      }
      totalCount += count.longValue();
      totalAmount += amount.longValue();
      observedRows++;
    }

    final int expectedRowsPerKey = rollup ? 1 : ROLLUP_ROWS_PER_KEY;
    if (rowsPerKey.size() != ROLLUP_ROW_COUNT) {
      throw new IllegalStateException("Expected " + ROLLUP_ROW_COUNT + " keys, got " + rowsPerKey.size());
    }
    for (final Map.Entry<String, Integer> entry : rowsPerKey.entrySet()) {
      if (entry.getValue() != expectedRowsPerKey) {
        throw new IllegalStateException("Unexpected rows for " + entry.getKey() + ": " + entry.getValue());
      }
    }

    if (observedRows != expectedRows || totalCount != ROW_COUNT || totalAmount != expectedTotalAmount()) {
      throw new IllegalStateException(
          "Unexpected validation totals: rows=" + observedRows
          + ", count=" + totalCount
          + ", amount=" + totalAmount
      );
    }
  }

  private static int validateIndexedRow(final Row row)
  {
    final long timestamp = row.getTimestampFromEpoch();
    if (timestamp != TIMESTAMP_A && timestamp != TIMESTAMP_B) {
      throw new IllegalStateException("Unexpected timestamp: " + timestamp);
    }

    final List<String> string0 = row.getDimension(STRING_DIMENSION_0);
    if (string0.size() != 1 || string0.get(0) == null || !string0.get(0).startsWith("s")) {
      throw new IllegalStateException("Unexpected s0: " + string0);
    }
    final int group;
    try {
      group = Integer.parseInt(string0.get(0).substring(1));
    }
    catch (NumberFormatException e) {
      throw new IllegalStateException("Unexpected s0: " + string0, e);
    }
    if (group < 0 || group >= VALUE_CARDINALITY) {
      throw new IllegalStateException("Unexpected group: " + group);
    }

    expectDimension(row, LONG_DIMENSION_0, Long.toString(group));
    expectDimension(row, DOUBLE_DIMENSION_0, Double.toString(group + 0.5D));
    if ((group & 3) == 0) {
      if (!row.getDimension(STRING_DIMENSION_1).isEmpty()) {
        throw new IllegalStateException("Expected null s1 for group " + group);
      }
    } else {
      expectDimension(row, STRING_DIMENSION_1, "category" + (group & 3));
    }
    expectDimension(row, LONG_DIMENSION_1, Long.toString(group * 10L + 1L));
    expectDimension(row, DOUBLE_DIMENSION_1, Double.toString(group + 0.25D));
    return group;
  }

  private static void expectDimension(final Row row, final String name, final String expected)
  {
    final List<String> actual = row.getDimension(name);
    if (actual.size() != 1 || !expected.equals(actual.get(0))) {
      throw new IllegalStateException("Unexpected " + name + ": " + actual + ", expected " + expected);
    }
  }

  private void validateInputRowsAndIndex() throws IOException
  {
    boolean unusedPaddingRetained = false;
    int rowNumber = 0;
    while (rowNumber < ROW_COUNT) {
      source.setEntity(new ByteEntity(payloads[rowNumber]));
      int rowsInMessage = 0;
      try (final CloseableIterator<InputRow> rows = reader.read()) {
        while (rows.hasNext()) {
          final InputRow row = rows.next();
          if (rowsInMessage++ != 0) {
            throw new IllegalStateException("Expected one row per input entity");
          }
          validateInputRow(row, rowNumber);
          if (rowNumber == 0) {
            unusedPaddingRetained = row.getRaw(unusedPaddingName()) != null;
          }
          index.add(row);
          rowNumber++;
        }
      }
      if (rowsInMessage != 1) {
        throw new IllegalStateException("Expected one row per input entity, got " + rowsInMessage);
      }
    }

    if (rowNumber != ROW_COUNT) {
      throw new IllegalStateException("Expected " + ROW_COUNT + " parsed rows, got " + rowNumber);
    }
    final boolean legacyReader = reader.getClass().getSimpleName().equals("JsonReader");
    if (legacyReader != unusedPaddingRetained) {
      throw new IllegalStateException(
          "Unexpected unused padding state for " + reader.getClass().getName()
          + ": retained=" + unusedPaddingRetained
      );
    }
    validateIndex();
    System.out.println(
        "validated payloadBytes=" + payloadBytes
        + " rollup=" + rollup
        + " reader=" + reader.getClass().getName()
        + " unusedPaddingRetained=" + unusedPaddingRetained
    );
  }

  private static void validateInputRow(final InputRow row, final int rowNumber)
  {
    final long expectedTimestamp = timestampForRow(rowNumber);
    if (row.getTimestampFromEpoch() != expectedTimestamp) {
      throw new IllegalStateException(
          "Unexpected input timestamp at row " + rowNumber + ": " + row.getTimestampFromEpoch()
      );
    }
    if (!DIMENSION_NAMES.equals(row.getDimensions())) {
      throw new IllegalStateException("Unexpected input dimensions: " + row.getDimensions());
    }

    final int group = groupForRow(rowNumber);
    expectInputValue(row, STRING_DIMENSION_0, "s" + group, String.class);
    expectInputValue(row, LONG_DIMENSION_0, (long) group, Long.class);
    expectInputValue(row, DOUBLE_DIMENSION_0, group + 0.5D, Double.class);
    if ((group & 3) == 0) {
      if (row.getRaw(STRING_DIMENSION_1) != null) {
        throw new IllegalStateException("Expected null s1 at row " + rowNumber);
      }
    } else {
      expectInputValue(row, STRING_DIMENSION_1, "category" + (group & 3), String.class);
    }
    expectInputValue(row, LONG_DIMENSION_1, group * 10L + 1L, Long.class);
    expectInputValue(row, DOUBLE_DIMENSION_1, group + 0.25D, Double.class);
    expectInputValue(row, AMOUNT_METRIC, amountForGroup(group), Long.class);
  }

  private static void expectInputValue(
      final InputRow row,
      final String name,
      final Object expected,
      final Class<?> expectedType
  )
  {
    final Object actual = row.getRaw(name);
    if (!expectedType.isInstance(actual) || !expected.equals(actual)) {
      throw new IllegalStateException(
          "Unexpected input " + name + ": " + actual + " (" + (actual == null ? "null" : actual.getClass())
          + "), expected " + expected
      );
    }
  }

  private static int groupForRow(final int rowNumber)
  {
    return (rowNumber / TIMESTAMP_VARIANTS) % VALUE_CARDINALITY;
  }

  private static long timestampForRow(final int rowNumber)
  {
    return (rowNumber & 1) == 0 ? TIMESTAMP_A : TIMESTAMP_B;
  }

  private static String timestampTextForRow(final int rowNumber)
  {
    return timestampForRow(rowNumber) == TIMESTAMP_A ? TIMESTAMP_A_TEXT : TIMESTAMP_B_TEXT;
  }

  private static long amountForGroup(final int group)
  {
    return 100L + group;
  }

  private static long expectedTotalAmount()
  {
    long total = 0L;
    for (int rowNumber = 0; rowNumber < ROW_COUNT; rowNumber++) {
      total += amountForGroup(groupForRow(rowNumber));
    }
    return total;
  }

  private String unusedPaddingName()
  {
    return payloadBytes == WIDE_PAYLOAD_BYTES ? "padding00" : "padding";
  }

  private static byte[][] buildPayloads(final int payloadBytes)
  {
    final byte[][] payloads = new byte[ROW_COUNT][];
    for (int rowNumber = 0; rowNumber < ROW_COUNT; rowNumber++) {
      final byte[] rowBytes = buildRecord(payloadBytes, rowNumber);
      if (rowBytes.length != payloadBytes) {
        throw new IllegalStateException(
            "Expected record size " + payloadBytes + ", got " + rowBytes.length + " at row " + rowNumber
        );
      }
      payloads[rowNumber] = rowBytes;
    }
    return payloads;
  }

  private static byte[] buildRecord(final int payloadBytes, final int rowNumber)
  {
    final int group = groupForRow(rowNumber);
    final StringBuilder builder = new StringBuilder(payloadBytes);
    builder.append("{\"ts\":\"").append(timestampTextForRow(rowNumber)).append("\"");
    builder.append(",\"s0\":\"s").append(group).append("\"");
    builder.append(",\"l0\":").append(group);
    builder.append(",\"d0\":").append(Double.toString(group + 0.5D));
    builder.append(",\"s1\":");
    if ((group & 3) == 0) {
      builder.append("null");
    } else {
      builder.append("\"category").append(group & 3).append("\"");
    }
    builder.append(",\"l1\":").append(group * 10L + 1L);
    builder.append(",\"d1\":").append(Double.toString(group + 0.25D));
    builder.append(",\"amount\":").append(amountForGroup(group));

    if (payloadBytes == COMPACT_PAYLOAD_BYTES) {
      builder.append(",\"padding\":\"");
    } else {
      for (int padding = 0; padding < PADDING_FIELD_COUNT - 1; padding++) {
        builder.append(",\"padding").append(String.format("%02d", padding)).append("\":");
        builder.append("\"unused\"");
      }
      builder.append(",\"padding").append(String.format("%02d", PADDING_FIELD_COUNT - 1)).append("\":\"");
    }

    final int closingBytes = 2;
    final int fillerLength = payloadBytes - builder.length() - closingBytes;
    if (fillerLength < 0) {
      throw new IllegalStateException("Payload template exceeds target size " + payloadBytes);
    }
    builder.append("x".repeat(fillerLength)).append("\"}");
    return builder.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static void dumpPayloads(final Path directory) throws IOException
  {
    Files.createDirectories(directory);
    writePayloadFile(directory.resolve("payload-256.json"), buildPayloads(COMPACT_PAYLOAD_BYTES));
    writePayloadFile(directory.resolve("payload-4096.json"), buildPayloads(WIDE_PAYLOAD_BYTES));
  }

  private static void writePayloadFile(final Path path, final byte[][] payloads) throws IOException
  {
    final ByteArrayOutputStream output = new ByteArrayOutputStream(payloads[0].length * payloads.length);
    for (final byte[] payload : payloads) {
      output.write(payload, 0, payload.length);
    }
    Files.write(path, output.toByteArray());
  }

  public static void main(final String[] args) throws Exception
  {
    if (args.length == 0) {
      throw new IllegalArgumentException("Use --validate or --dump-payloads <directory>");
    }
    if ("--dump-payloads".equals(args[0])) {
      if (args.length != 2) {
        throw new IllegalArgumentException("Use --dump-payloads <directory>");
      }
      dumpPayloads(Path.of(args[1]));
      System.out.println("dumped payloads to " + args[1]);
      return;
    }
    if (!"--validate".equals(args[0]) || args.length > 1) {
      throw new IllegalArgumentException("Use --validate or --dump-payloads <directory>");
    }

    for (final int requestedPayloadBytes : List.of(COMPACT_PAYLOAD_BYTES, WIDE_PAYLOAD_BYTES)) {
      for (final boolean requestedRollup : List.of(true, false)) {
        final SchemaDirectedJsonBenchmark state = new SchemaDirectedJsonBenchmark();
        state.payloadBytes = requestedPayloadBytes;
        state.rollup = requestedRollup;
        state.setupPayload();
        state.setupIndex();
        try {
          state.validateInputRowsAndIndex();
        }
        finally {
          if (state.index != null) {
            state.index.close();
          }
        }
      }
    }
  }
}
