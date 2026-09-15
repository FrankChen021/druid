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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import java.io.IOException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Experimental fixed-schema reader: materialize selected scalar fields directly into ordinal slots.
 * Unsupported value shapes and failures replay through the existing reader to preserve its behavior.
 */
class SchemaDirectedJsonReader extends JsonReader
{
  private final InputRowSchema schema;
  private final JsonReader legacy;
  private final JsonFactory factory = new JsonFactory();
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
    this.schema = schema;
    this.legacy = new JsonReader(schema, source, JSONPathSpec.DEFAULT, mapper, keepNullColumns);
    this.keepNullColumns = keepNullColumns;
    final Map<String, Integer> compiledPositions = new LinkedHashMap<>();
    for (final String field : ((ColumnsFilter.InclusionBased) schema.getColumnsFilter()).getInclusions()) {
      compiledPositions.put(field, compiledPositions.size());
    }
    compiledPositions.computeIfAbsent(schema.getTimestampSpec().getTimestampColumn(), ignored -> compiledPositions.size());
    this.positions = Map.copyOf(compiledPositions);
  }

  @Override
  protected List<InputRow> parseInputRows(final InputEntity entity) throws IOException
  {
    try {
      final List<InputRow> rows = new ArrayList<>();
      try (final JsonParser parser = factory.createParser(entity.open())) {
        while (parser.nextToken() != null) {
          if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new UnsupportedOperationException("Use the tree reader for this record");
          }
          final Object[] values = new Object[positions.size()];
          final boolean[] present = new boolean[positions.size()];
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.currentToken() != JsonToken.FIELD_NAME) {
              throw new UnsupportedOperationException("Use the tree reader for this record");
            }
            final Integer position = positions.get(parser.currentName());
            final JsonToken token = parser.nextToken();
            if (position == null) {
              if (token == JsonToken.VALUE_STRING) {
                // Finish decoding to enforce string limits without allocating a String for the unused value.
                parser.streamReadConstraints().validateStringLength(parser.getTextLength());
              } else if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                throw new UnsupportedOperationException("Use the tree reader for nested records");
              }
            } else {
              final Object value;
              switch (token) {
                case VALUE_STRING:
                  value = JSONFlattenerMaker.charsetFix(parser.getText(), encoder);
                  break;
                case VALUE_NUMBER_INT:
                  final JsonParser.NumberType numberType = parser.getNumberType();
                  if (numberType == JsonParser.NumberType.INT || numberType == JsonParser.NumberType.LONG) {
                    value = parser.getLongValue();
                  } else {
                    value = parser.getDoubleValue();
                  }
                  break;
                case VALUE_NUMBER_FLOAT:
                  value = parser.getDoubleValue();
                  break;
                case VALUE_TRUE:
                  value = true;
                  break;
                case VALUE_FALSE:
                  value = false;
                  break;
                case VALUE_NULL:
                  value = null;
                  break;
                default:
                  throw new UnsupportedOperationException("Use the tree reader for this record");
              }
              values[position] = value;
              present[position] = keepNullColumns || value != null;
            }
          }
          rows.add(MapInputRowParser.parse(
              schema.getTimestampSpec(),
              schema.getDimensionsSpec().getDimensionNames(),
              new SlotMap(positions, values, present)
          ));
        }
      }
      if (rows.isEmpty()) {
        throw new UnsupportedOperationException("Use the tree reader for empty input");
      }
      return rows;
    }
    catch (IOException | RuntimeException e) {
      // Reparse exceptional records through the established path, including full event/error reporting.
      return legacy.parseInputRows(entity);
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return legacy.sample();
  }

  private static class SlotMap extends AbstractMap<String, Object>
  {
    private final Map<String, Integer> positions;
    private final Object[] values;
    private final boolean[] present;

    SlotMap(final Map<String, Integer> positions, final Object[] values, final boolean[] present)
    {
      this.positions = positions;
      this.values = values;
      this.present = present;
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
      final Set<Entry<String, Object>> entries = new LinkedHashSet<>();
      for (final Entry<String, Integer> field : positions.entrySet()) {
        if (present[field.getValue()]) {
          entries.add(new SimpleImmutableEntry<>(field.getKey(), values[field.getValue()]));
        }
      }
      return entries;
    }
  }
}
