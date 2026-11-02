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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Experimental fixed-schema reader: materialize selected scalar fields directly into ordinal slots.
 * Unsupported value shapes and failures are reparsed by the parent {@link JsonReader} to preserve its behavior.
 */
class SchemaDirectedJsonReader extends JsonReader
{
  private final Map<String, Integer> positions;
  private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
  private final boolean keepNullColumns;

  SchemaDirectedJsonReader(
      final InputRowSchema schema,
      final InputEntity source,
      final ObjectMapper mapper,
      final boolean keepNullColumns
  )
  {
    super(schema, source, JSONPathSpec.DEFAULT, mapper, keepNullColumns);

    this.keepNullColumns = keepNullColumns;
    this.positions = new HashMap<>();
    for (final String field : ((ColumnsFilter.InclusionBased) schema.getColumnsFilter()).getInclusions()) {
      this.positions.put(field, positions.size());
    }
    this.positions.putIfAbsent(schema.getTimestampSpec().getTimestampColumn(), positions.size());
  }

  /**
   * Whether this reader supports the given schema and flatten spec. It requires the dimensions and the required columns
   * to be known in advance, and cannot apply flatten fields.
   */
  static boolean isSupported(final InputRowSchema schema, @Nullable final JSONPathSpec flattenSpec)
  {
    return schema.getDimensionsSpec().hasFixedDimensions()
           && schema.getColumnsFilter() instanceof ColumnsFilter.InclusionBased
           && (flattenSpec == null || (flattenSpec.isUseFieldDiscovery() && flattenSpec.getFields().isEmpty()));
  }

  @Override
  protected List<InputRow> parseInputRows(final InputEntity entity) throws IOException
  {
    try {
      final List<InputRow> rows = new ArrayList<>();
      try (final JsonParser parser = jsonFactory.createParser(entity.open())) {
        while (parser.nextToken() != null) {
          rows.add(parseRow(parser));
        }
      }
      if (rows.isEmpty()) {
        throw new UnsupportedOperationException("Use the tree reader for empty input");
      }
      return rows;
    }
    catch (IOException | RuntimeException e) {
      // Reparse exceptional records through the established path, including full event/error reporting.
      return super.parseInputRows(entity);
    }
  }

  /**
   * Parses the JSON object at the current token of the parser into a row, keeping only the fields required by the
   * schema.
   */
  private InputRow parseRow(final JsonParser parser) throws IOException
  {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      throw new UnsupportedOperationException("Use the tree reader for this record");
    }

    final Object[] values = new Object[positions.size()];
    final int[] fieldOrder = new int[positions.size()];
    int fieldCount = 0;
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.currentToken() != JsonToken.FIELD_NAME) {
        throw new UnsupportedOperationException("Use the tree reader for this record");
      }

      final Integer position = positions.get(parser.currentName());
      final JsonToken token = parser.nextToken();
      if (position == null) {
        skipUnusedColumn(parser, token);
      } else {
        values[position] = parseColumnValue(parser, token);
        if (fieldOrder[position] == 0) {
          fieldOrder[position] = ++fieldCount;
        }
      }
    }
    return MapInputRowParser.parse(
        inputRowSchema.getTimestampSpec(),
        inputRowSchema.getDimensionsSpec().getDimensionNames(),
        new SlotMap(positions, values, fieldOrder, keepNullColumns)
    );
  }

  /**
   * Parses the value of a required column at the current token of the parser, converted the same way as the tree
   * reader. Objects and arrays are not supported and throw, so that the record falls back to the tree reader.
   */
  @Nullable
  private Object parseColumnValue(final JsonParser parser, final JsonToken token) throws IOException
  {
    switch (token) {
      case VALUE_STRING:
        return JSONFlattenerMaker.charsetFix(parser.getText(), encoder);
      case VALUE_NUMBER_INT:
        final JsonParser.NumberType numberType = parser.getNumberType();
        if (numberType == JsonParser.NumberType.INT || numberType == JsonParser.NumberType.LONG) {
          return parser.getLongValue();
        } else {
          return parser.getDoubleValue();
        }
      case VALUE_NUMBER_FLOAT:
        return parser.getDoubleValue();
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      case VALUE_NULL:
        return null;
      default:
        throw new UnsupportedOperationException("Use the tree reader for this record");
    }
  }

  /**
   * Skips the value of a field that is not required by the schema, including nested objects and arrays, while
   * enforcing the same limits the tree reader enforces when it reads the value.
   */
  private void skipUnusedColumn(final JsonParser parser, final JsonToken token) throws IOException
  {
    int depth = 0;
    JsonToken current = token;
    while (true) {
      if (current == null) {
        throw new UnsupportedOperationException("Use the tree reader for truncated records");
      } else if (current == JsonToken.START_OBJECT || current == JsonToken.START_ARRAY) {
        depth++;
      } else if (current == JsonToken.END_OBJECT || current == JsonToken.END_ARRAY) {
        depth--;
      } else if (current == JsonToken.VALUE_STRING) {
        // The tree reader builds every value, so it rejects a record whose string exceeds
        // StreamReadConstraints#getMaxStringLength even if the field is unused. Skipping over a string does not check
        // that limit, so without this check the record would be accepted here but rejected by the tree reader, and
        // allowOptimizedReader would change which records are ingested. getTextLength() decodes the string without
        // allocating a String; validateStringLength() throws if it is too long, and the record then falls back to
        // the tree reader, which reports the same error.
        parser.streamReadConstraints().validateStringLength(parser.getTextLength());
      }
      if (depth == 0) {
        return;
      }
      current = parser.nextToken();
    }
  }

  private static class SlotMap extends AbstractMap<String, Object>
  {
    private final Map<String, Integer> positions;
    private final Object[] values;
    private final int[] fieldOrder;
    private final boolean keepNullColumns;

    SlotMap(
        final Map<String, Integer> positions,
        final Object[] values,
        final int[] fieldOrder,
        final boolean keepNullColumns
    )
    {
      this.positions = positions;
      this.values = values;
      this.fieldOrder = fieldOrder;
      this.keepNullColumns = keepNullColumns;
    }

    @Override
    public Object get(final Object key)
    {
      final Integer position = positions.get(key);
      return position == null ? null : values[position];
    }

    @Override
    public Set<Entry<String, Object>> entrySet()
    {
      // Preserve JSON encounter order for downstream parse-error input reporting.
      final String[] orderedFields = new String[positions.size()];
      for (final Entry<String, Integer> field : positions.entrySet()) {
        final int order = fieldOrder[field.getValue()];
        if (order != 0) {
          orderedFields[order - 1] = field.getKey();
        }
      }
      final Set<Entry<String, Object>> entries = new LinkedHashSet<>();
      for (final String field : orderedFields) {
        if (field != null) {
          final Object value = values[positions.get(field)];
          if (keepNullColumns || value != null) {
            entries.add(new SimpleImmutableEntry<>(field, value));
          }
        }
      }
      return entries;
    }
  }
}
