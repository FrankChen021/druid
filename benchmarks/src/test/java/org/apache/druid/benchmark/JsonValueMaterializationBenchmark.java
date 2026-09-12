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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
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
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Compares the PR's streaming JSON tree/flattener path with two flat-record materialization prototypes.
 *
 * <p>The tree case uses the public {@link JsonInputFormat} API and a reused reader, exactly as the value side of
 * Kafka ingestion does.  The other cases still create a Druid {@link InputRow} and consume row fields, but replace
 * the tree/flattener stage with Jackson map materialization.  {@code jackson-map} is intentionally a semantic
 * control: Jackson's default integer node type is not the {@code Long} produced by {@code JSONFlattenerMaker}.  The
 * token-loop implementation normalizes integral and floating-point tokens to {@code Long} and {@code Double}, but is
 * exploratory and only supports flat scalar records.  {@code token-map-normalized} additionally routes strings
 * through the production flattener's UTF-8/surrogate conversion.  Neither prototype handles flattenSpec, transforms,
 * metrics, or nested values as the production reader does.</p>
 *
 * <p>The record-shape parameter uses three workloads: a compact record with all configured dimensions consumed, a
 * wide record with all configured dimensions consumed, and the same wide record with only two configured fields
 * consumed.  The sparse case is useful for showing the lazy value extraction of the production flattener; it is not a
 * claim about the complete downstream ingestion pipeline.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class JsonValueMaterializationBenchmark
{
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>()
  {
  };

  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(
      "__kif_auto_timestamp",
      "auto",
      DateTimes.EPOCH
  );

  @Param({"tree-flattener", "jackson-map", "token-map", "token-map-normalized"})
  private String materializer;

  @Param({"256", "4096"})
  private int messageSize;

  @Param({"explicit", "discovery"})
  private String dimensionMode;

  @Param({"compact-all", "wide-all", "wide-sparse"})
  private String recordShape;

  private ObjectMapper mapper;
  private JsonFactory jsonFactory;
  private CharsetEncoder utf8Encoder;
  private InputRowSchema inputRowSchema;
  private SettableByteEntity<ByteEntity> source;
  private ByteEntity entity;
  private InputEntityReader reader;
  private byte[] messageBytes;
  private List<String> explicitDimensions;
  private List<String> fieldsToConsume;
  private Set<String> fieldsToMaterialize;

  @Setup
  public void setUp()
  {
    final RecordSpec recordSpec = RecordSpec.create(messageSize, recordShape);
    messageBytes = recordSpec.bytes;
    explicitDimensions = recordSpec.explicitDimensions;
    fieldsToConsume = "discovery".equals(dimensionMode) && recordShape.endsWith("-all")
                      ? recordSpec.allFields
                      : recordSpec.fieldsToConsume;
    fieldsToMaterialize = Set.copyOf(explicitDimensions);

    final DimensionsSpec dimensionsSpec;
    if ("discovery".equals(dimensionMode)) {
      dimensionsSpec = DimensionsSpec.builder().useSchemaDiscovery(true).build();
      fieldsToMaterialize = Set.of();
    } else {
      dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(explicitDimensions));
    }
    inputRowSchema = new InputRowSchema(TIMESTAMP_SPEC, dimensionsSpec, ColumnsFilter.all());

    mapper = new ObjectMapper();
    jsonFactory = new JsonFactory();
    utf8Encoder = StandardCharsets.UTF_8.newEncoder();

    if ("tree-flattener".equals(materializer)) {
      source = new SettableByteEntity<>();
      entity = new ByteEntity(messageBytes);
      source.setEntity(entity);
      reader = new JsonInputFormat(null, null, null, false, false, false)
          .createReader(inputRowSchema, source, null);
    }
  }

  @Benchmark
  public void parseAndConsume(final Blackhole blackhole) throws IOException
  {
    final InputRow row;
    switch (materializer) {
      case "tree-flattener":
        row = readTreeRow();
        break;
      case "jackson-map":
        row = MapInputRowParser.parse(inputRowSchema, mapper.readValue(messageBytes, MAP_TYPE));
        break;
      case "token-map":
      case "token-map-normalized":
        row = MapInputRowParser.parse(inputRowSchema, readTokenMap());
        break;
      default:
        throw new IllegalStateException("Unknown materializer: " + materializer);
    }

    consumeRow(row, blackhole);
  }

  private InputRow readTreeRow() throws IOException
  {
    source.setEntity(entity);
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      if (!rows.hasNext()) {
        throw new IllegalStateException("Expected one row");
      }
      final InputRow row = rows.next();
      if (rows.hasNext()) {
        throw new IllegalStateException("Expected exactly one row");
      }
      return row;
    }
  }

  /**
   * Reads only the fields needed by a fixed-dimension schema.  Discovery keeps every root field.  This deliberately
   * skips child tokens for unselected fields and therefore is only valid for flat records with no flattenSpec.
   */
  private Map<String, Object> readTokenMap() throws IOException
  {
    final Map<String, Object> values = new LinkedHashMap<>();
    try (final JsonParser parser = jsonFactory.createParser(messageBytes)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IOException("Expected a JSON object");
      }
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if (parser.currentToken() != JsonToken.FIELD_NAME) {
          throw new IOException("Expected a JSON field");
        }
        final String field = parser.currentName();
        final JsonToken valueToken = parser.nextToken();
        if (fieldsToMaterialize.isEmpty() || fieldsToMaterialize.contains(field)) {
          values.put(field, readScalar(parser, valueToken));
        } else {
          parser.skipChildren();
        }
      }
    }
    return values;
  }

  private Object readScalar(final JsonParser parser, final JsonToken token) throws IOException
  {
    switch (token) {
      case VALUE_STRING:
        final String text = parser.getText();
        if ("token-map-normalized".equals(materializer)) {
          return JSONFlattenerMaker.convertJsonNode(TextNode.valueOf(text), utf8Encoder);
        }
        return text;
      case VALUE_NUMBER_INT:
        return parser.getLongValue();
      case VALUE_NUMBER_FLOAT:
        return parser.getDoubleValue();
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      default:
        throw new IOException("token-map only supports flat scalar values, got " + token);
    }
  }

  private void consumeRow(final InputRow row, final Blackhole blackhole)
  {
    blackhole.consume(row.getTimestampFromEpoch());
    // Force the dimension-list path even in sparse mode; only raw value reads are sparse.
    final List<String> dimensions = row.getDimensions();
    blackhole.consume(dimensions.size());
    for (final String field : fieldsToConsume) {
      blackhole.consume(row.getRaw(field));
    }
  }

  private static final class RecordSpec
  {
    private final byte[] bytes;
    private final List<String> allFields;
    private final List<String> explicitDimensions;
    private final List<String> fieldsToConsume;

    private RecordSpec(
        final byte[] bytes,
        final List<String> allFields,
        final List<String> explicitDimensions,
        final List<String> fieldsToConsume
    )
    {
      this.bytes = bytes;
      this.allFields = allFields;
      this.explicitDimensions = explicitDimensions;
      this.fieldsToConsume = fieldsToConsume;
    }

    private static RecordSpec create(final int targetSize, final String shape)
    {
      final boolean wide = shape.startsWith("wide-");
      final boolean sparse = shape.endsWith("sparse");
      final int fieldCount;
      if (!wide) {
        fieldCount = 0;
      } else if (targetSize >= 4096) {
        fieldCount = 64;
      } else {
        fieldCount = 16;
      }
      final StringBuilder json = new StringBuilder(targetSize + 32);
      final List<String> allFields = new ArrayList<>();
      json.append('{');
      appendStringField(json, "id", "streaming-id");
      allFields.add("id");
      json.append(',');
      appendStringField(json, "site", "sg");
      allFields.add("site");
      json.append(",\"count\":42,\"ratio\":3.5,\"active\":true");
      allFields.add("count");
      allFields.add("ratio");
      allFields.add("active");
      if (wide) {
        for (int i = 0; i < fieldCount; i++) {
          final String field = String.format("f%02d", i);
          allFields.add(field);
          json.append(',');
          appendWideField(json, field, i);
        }
      }
      json.append(",\"payload\":\"");
      allFields.add("payload");
      final int suffixLength = 2;
      final int paddingLength = targetSize - json.length() - suffixLength;
      if (paddingLength < 0) {
        throw new IllegalArgumentException(
            "record shape " + shape + " exceeds target size " + targetSize + " by " + -paddingLength + " bytes"
        );
      }
      json.append("x".repeat(paddingLength)).append("\"}");
      final byte[] bytes = json.toString().getBytes(StandardCharsets.UTF_8);
      if (bytes.length != targetSize) {
        throw new IllegalStateException("Expected " + targetSize + " bytes, got " + bytes.length);
      }

      final List<String> explicitDimensions;
      if (wide) {
        // Five configured dimensions leave eleven unconfigured fields in the wide record for projection tests.
        explicitDimensions = ImmutableList.of("f00", "f01", "f02", "f03", "f04", "payload");
      } else {
        explicitDimensions = ImmutableList.of("id", "site", "count", "ratio", "active", "payload");
      }
      final List<String> fieldsToConsume;
      if (sparse) {
        fieldsToConsume = ImmutableList.of(explicitDimensions.get(0), "payload");
      } else {
        fieldsToConsume = explicitDimensions;
      }
      return new RecordSpec(bytes, allFields, explicitDimensions, fieldsToConsume);
    }

    private static void appendStringField(final StringBuilder json, final String field, final String value)
    {
      json.append('"').append(field).append("\":\"").append(value).append('"');
    }

    private static void appendWideField(final StringBuilder json, final String field, final int index)
    {
      json.append('"').append(field).append("\":");
      switch (index % 4) {
        case 0:
          json.append(index);
          break;
        case 1:
          json.append('"').append("v").append(index).append('"');
          break;
        case 2:
          json.append(index).append('.').append(index + 5);
          break;
        case 3:
          json.append(index % 2 == 1 ? "true" : "false");
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  public static void main(final String[] args) throws RunnerException
  {
    if (args.length == 1 && "--validate".equals(args[0])) {
      try {
        validateFixtures();
      }
      catch (IOException e) {
        throw new IllegalStateException("Fixture validation failed", e);
      }
      return;
    }
    final Options options = new OptionsBuilder()
        .include(JsonValueMaterializationBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }

  private static void validateFixtures() throws IOException
  {
    final String[] materializers = {"tree-flattener", "jackson-map", "token-map", "token-map-normalized"};
    final int[] messageSizes = {256, 4096};
    final String[] dimensionModes = {"explicit", "discovery"};
    final String[] recordShapes = {"compact-all", "wide-all", "wide-sparse"};
    int cases = 0;

    for (final String materializer : materializers) {
      for (final int messageSize : messageSizes) {
        for (final String dimensionMode : dimensionModes) {
          for (final String recordShape : recordShapes) {
            final JsonValueMaterializationBenchmark benchmark = new JsonValueMaterializationBenchmark();
            benchmark.materializer = materializer;
            benchmark.messageSize = messageSize;
            benchmark.dimensionMode = dimensionMode;
            benchmark.recordShape = recordShape;
            benchmark.setUp();
            final InputRow row;
            switch (materializer) {
              case "tree-flattener":
                row = benchmark.readTreeRow();
                break;
              case "jackson-map":
                row = MapInputRowParser.parse(benchmark.inputRowSchema, benchmark.mapper.readValue(
                    benchmark.messageBytes,
                    MAP_TYPE
                ));
                break;
              case "token-map":
              case "token-map-normalized":
                row = MapInputRowParser.parse(benchmark.inputRowSchema, benchmark.readTokenMap());
                break;
              default:
                throw new AssertionError(materializer);
            }
            final RecordSpec spec = RecordSpec.create(messageSize, recordShape);
            final List<String> expectedDimensions = "discovery".equals(dimensionMode)
                                                     ? spec.allFields
                                                     : spec.explicitDimensions;
            final String context = materializer + "/" + messageSize + "/" + dimensionMode + "/" + recordShape;
            check(row.getTimestampFromEpoch() == 0, context + " timestamp");
            check(expectedDimensions.equals(row.getDimensions()), context + " dimensions");
            if (!recordShape.startsWith("wide-") || "discovery".equals(dimensionMode)) {
              checkValue(row, "id", "streaming-id", context);
              checkValue(row, "site", "sg", context);
              checkNumber(row, "count", 42L, context);
              checkNumber(row, "ratio", 3.5D, context);
              checkValue(row, "active", Boolean.TRUE, context);
            }
            checkPadding(row, context);
            if (recordShape.startsWith("wide-")) {
              checkNumber(row, "f00", 0L, context);
              checkValue(row, "f01", "v1", context);
              checkNumber(row, "f02", 2.7D, context);
              checkValue(row, "f03", Boolean.TRUE, context);
              checkNumber(row, "f04", 4L, context);
            }
            if ("tree-flattener".equals(materializer) || "token-map".equals(materializer)
                || "token-map-normalized".equals(materializer)) {
              if (!recordShape.startsWith("wide-") || "discovery".equals(dimensionMode)) {
                checkClass(row, "count", Long.class, context);
                checkClass(row, "ratio", Double.class, context);
              }
              if (recordShape.startsWith("wide-")) {
                checkClass(row, "f00", Long.class, context);
                checkClass(row, "f02", Double.class, context);
              }
            }
            cases++;
          }
        }
      }
    }
    System.out.println("Validated " + cases + " fixture rows across all materializers, sizes, shapes, and dimension modes");
  }

  private static void checkValue(
      final InputRow row,
      final String field,
      final Object expected,
      final String context
  )
  {
    check(expected.equals(row.getRaw(field)), context + " " + field + " value");
  }

  private static void checkNumber(
      final InputRow row,
      final String field,
      final double expected,
      final String context
  )
  {
    final Object actual = row.getRaw(field);
    check(
        actual instanceof Number && Double.compare(((Number) actual).doubleValue(), expected) == 0,
        context + " " + field + " number (actual=" + actual + ")"
    );
  }

  private static void checkPadding(final InputRow row, final String context)
  {
    final Object actual = row.getRaw("payload");
    check(actual instanceof String && !((String) actual).isEmpty(), context + " payload type");
    for (int i = 0; i < ((String) actual).length(); i++) {
      check(((String) actual).charAt(i) == 'x', context + " payload contents");
    }
  }

  private static void checkClass(
      final InputRow row,
      final String field,
      final Class<?> expected,
      final String context
  )
  {
    final Object actual = row.getRaw(field);
    check(actual != null && expected.equals(actual.getClass()), context + " " + field + " class (actual=" + actual + ")");
  }

  private static void check(final boolean condition, final String description)
  {
    if (!condition) {
      throw new IllegalStateException("Fixture validation failed for " + description);
    }
  }
}
