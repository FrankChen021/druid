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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class SchemaDirectedJsonReaderTest extends InitializedNullHandlingTest
{
  private static final List<String> DIMENSIONS = List.of("s", "n", "b");
  private static final Set<String> REQUIRED = Set.of("ts", "s", "n", "b", "amount", "transform_input");

  private static InputRowSchema schema()
  {
    return new InputRowSchema(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS)),
        ColumnsFilter.inclusionBased(REQUIRED)
    );
  }

  public static Stream<String> records()
  {
    return Stream.of(
        "{\"ts\":\"2021-06-25T00:00:00.001Z\",\"s\":\"hello\",\"n\":123,\"b\":true,\"amount\":45,\"transform_input\":9,\"unused\":\"ignored\"}",
        "{\"ts\":\"2021-06-25\",\"s\":null,\"n\":-9223372036854775808,\"b\":false}",
        "{\"ts\":\"2021-06-25\",\"n\":9223372036854775808,\"amount\":1.5}",
        "{\"ts\":\"2021-06-25\",\"s\":\"\\uD800\",\"n\":1e309}",
        "{\"ts\":\"2021-06-25\",\"s\":\"\\uD83D\\uDE00\"}",
        "{\"ts\":\"2021-06-25\",\"s\":\"before\",\"s\":null,\"s\":\"after\"}",
        "{\"ts\":\"2021-06-25\",\"unused\":{\"nested\":[1,{\"x\":true}]}}",
        "{\"ts\":\"2021-06-25\",\"s\":[\"a\",\"b\"]}",
        "{\"ts\":\"2021-06-25\",\"s\":{\"nested\":1}}",
        "{\"ts\":\"2021-06-25\"}\n{\"ts\":\"2021-06-26\",\"amount\":7}",
        "[{\"ts\":\"2021-06-25\"}]",
        "{\"ts\":\"bad\",\"unused\":\"included in error\"}",
        "{\"s\":\"missing timestamp\"}",
        "{\"ts\":\"2021-06-25\"} {\"ts\":",
        "{\"ts\":\"2021-06-25\",\"unused\": [1,]}",
        "",
        "{\"ts\":\"2021-06-25\",\"unused\":" + "1".repeat(1001) + "}"
    );
  }

  @ParameterizedTest
  @MethodSource("records")
  public void testMatchesLegacyRequiredValuesAndErrors(final String json) throws Exception
  {
    for (final boolean keepNulls : List.of(false, true)) {
      final ByteEntity source = new ByteEntity(StringUtils.toUtf8(json));
      final JsonReader baseline = new JsonReader(schema(), source, JSONPathSpec.DEFAULT, new ObjectMapper(), keepNulls);
      final SchemaDirectedJsonReader candidate = new SchemaDirectedJsonReader(schema(), source, new ObjectMapper(), keepNulls);
      final List<InputRow> expected;
      try {
        expected = read(baseline);
      }
      catch (Exception expectedError) {
        final Exception actual = Assertions.assertThrows(Exception.class, () -> read(candidate));
        Assertions.assertEquals(expectedError.getClass(), actual.getClass());
        Assertions.assertEquals(expectedError.getMessage(), actual.getMessage());
        continue;
      }
      final List<InputRow> actual = read(candidate);
      Assertions.assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++) {
        Assertions.assertEquals(expected.get(i).getTimestamp(), actual.get(i).getTimestamp());
        Assertions.assertEquals(expected.get(i).getDimensions(), actual.get(i).getDimensions());
        for (final String field : REQUIRED) {
          Assertions.assertEquals(expected.get(i).getRaw(field), actual.get(i).getRaw(field), field);
          if (expected.get(i).getRaw(field) != null) {
            Assertions.assertEquals(expected.get(i).getRaw(field).getClass(), actual.get(i).getRaw(field).getClass());
          }
        }
      }
    }
  }

  public static Stream<String> orderedRecords()
  {
    return Stream.of(
        "{\"ts\":\"2021-06-25\",\"s\":\"first\",\"n\":2,\"amount\":\"notanumber\"}",
        "{\"amount\":\"notanumber\",\"n\":2,\"s\":\"first\",\"ts\":\"2021-06-25\"}",
        "{\"s\":null,\"ts\":\"2021-06-25\",\"n\":2,\"s\":\"last\"}",
        "{\"s\":\"first\",\"ts\":\"2021-06-25\",\"n\":2,\"s\":null}"
    );
  }

  @ParameterizedTest
  @MethodSource("orderedRecords")
  public void testEventReportingPreservesInputOrder(final String json) throws Exception
  {
    for (final boolean keepNulls : List.of(false, true)) {
      final ByteEntity source = new ByteEntity(StringUtils.toUtf8(json));
      final MapBasedInputRow expected = (MapBasedInputRow) read(
          new JsonReader(schema(), source, JSONPathSpec.DEFAULT, new ObjectMapper(), keepNulls)
      ).get(0);
      final MapBasedInputRow actual = (MapBasedInputRow) read(
          new SchemaDirectedJsonReader(schema(), source, new ObjectMapper(), keepNulls)
      ).get(0);
      Assertions.assertEquals(expected.getEvent().toString(), actual.getEvent().toString());
    }
  }

  @Test
  public void testUnusedStringLimitStillRejectsRecord()
  {
    final String json = "{\"ts\":\"2021-06-25\",\"unused\":\""
                        + "x".repeat(StreamReadConstraints.defaults().getMaxStringLength() + 1)
                        + "\"}";
    final ByteEntity source = new ByteEntity(StringUtils.toUtf8(json));
    final Exception expected = Assertions.assertThrows(
        Exception.class,
        () -> read(new JsonReader(schema(), source, JSONPathSpec.DEFAULT, new ObjectMapper(), false))
    );
    final Exception actual = Assertions.assertThrows(
        Exception.class,
        () -> read(new SchemaDirectedJsonReader(schema(), source, new ObjectMapper(), false))
    );
    Assertions.assertEquals(expected.getClass(), actual.getClass());
    Assertions.assertEquals(expected.getMessage(), actual.getMessage());
  }

  @Test
  public void testDispatchAndSampling() throws Exception
  {
    final JsonInputFormat format = new JsonInputFormat(null, null, null, false, false, false);
    final ByteEntity source = new ByteEntity(StringUtils.toUtf8("{\"ts\":\"2021-06-25\",\"unused\":1}"));
    final InputEntityReader reader = format.createReader(schema(), source, null);
    Assertions.assertInstanceOf(SchemaDirectedJsonReader.class, reader);
    try (final CloseableIterator<InputRowListPlusRawValues> samples = reader.sample()) {
      Assertions.assertEquals(1, samples.next().getRawValues().get("unused"));
    }
    Assertions.assertEquals(
        JsonReader.class,
        format.createReader(
            new InputRowSchema(schema().getTimestampSpec(), schema().getDimensionsSpec(), ColumnsFilter.all()),
            source,
            null
        ).getClass()
    );
    Assertions.assertEquals(
        JsonReader.class,
        format.createReader(
            new InputRowSchema(schema().getTimestampSpec(), DimensionsSpec.EMPTY, ColumnsFilter.inclusionBased(REQUIRED)),
            source,
            null
        ).getClass()
    );
  }

  @Test
  public void testUnsupportedConfigurationsUseExistingReaders()
  {
    final ByteEntity source = new ByteEntity(new byte[0]);
    for (final JSONPathSpec flatten : List.of(
        new JSONPathSpec(false, List.of()),
        new JSONPathSpec(true, List.of(JSONPathFieldSpec.createNestedField("s", "$.nested.s")))
    )) {
      Assertions.assertEquals(
          JsonReader.class,
          new JsonInputFormat(flatten, null, null, false, false, false).createReader(schema(), source, null).getClass()
      );
    }
    Assertions.assertEquals(
        JsonNodeReader.class,
        new JsonInputFormat(null, null, null, false, false, true).createReader(schema(), source, null).getClass()
    );
    Assertions.assertEquals(
        JsonLineReader.class,
        new JsonInputFormat(null, null, null, false, true, false).createReader(schema(), source, null).getClass()
    );
  }

  @Test
  public void testTransformInputOutsideDimensions() throws Exception
  {
    final ByteEntity source = new ByteEntity(StringUtils.toUtf8(
        "{\"ts\":\"2021-06-25\",\"transform_input\":7,\"unused\":99}"
    ));
    final InputRow row = read(new JsonInputFormat(null, null, null, false, false, false)
        .createReader(schema(), source, null)).get(0);
    final TransformSpec transform = new TransformSpec(
        null,
        List.of(new ExpressionTransform("n", "transform_input + 1", ExprMacroTable.nil()))
    );
    Assertions.assertEquals(8L, transform.toTransformer().transform(row).getRaw("n"));
  }

  @Test
  public void testRowsSurviveSourceReuse() throws Exception
  {
    final MutableEntity source = new MutableEntity();
    source.data = StringUtils.toUtf8("{\"ts\":\"2021-06-25\",\"s\":\"first\",\"amount\":8}");
    final InputEntityReader reader = new JsonInputFormat(null, null, null, false, false, false)
        .createReader(schema(), source, null);
    final InputRow first = read(reader).get(0);
    source.data = StringUtils.toUtf8("{\"ts\":\"2021-06-26\",\"s\":\"second\",\"amount\":9}");
    Assertions.assertEquals("second", read(reader).get(0).getRaw("s"));
    Assertions.assertEquals("first", first.getRaw("s"));
    Assertions.assertEquals(8L, first.getRaw("amount"));
  }

  private static List<InputRow> read(final InputEntityReader reader) throws Exception
  {
    final List<InputRow> result = new ArrayList<>();
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      while (rows.hasNext()) {
        result.add(rows.next());
      }
    }
    return result;
  }

  private static class MutableEntity implements InputEntity
  {
    private byte[] data;

    @Override
    public URI getUri()
    {
      return null;
    }

    @Override
    public InputStream openRaw()
    {
      return new ByteArrayInputStream(data);
    }
  }
}
