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

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class JsonReaderBufferTest
{
  private static final byte[] DATA = StringUtils.toUtf8("{\"key\":\"café\"} {\"key\":\"東京\"}");
  private static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec("timestamp", "auto", DateTimes.EPOCH),
      DimensionsSpec.builder().useSchemaDiscovery(true).build(),
      ColumnsFilter.all()
  );

  @ParameterizedTest
  @CsvSource({"false,heap", "true,heap", "false,slice", "true,slice",
              "false,direct", "true,direct", "false,readonly", "true,readonly"})
  public void testBufferBoundsAndRepeatedReads(final boolean nodes, final String kind) throws IOException
  {
    final ByteBuffer storage = "direct".equals(kind)
                               ? ByteBuffer.allocateDirect(DATA.length + 8)
                               : ByteBuffer.allocate(DATA.length + 8);
    storage.position(3);
    storage.put(DATA);
    storage.limit(storage.position());
    storage.position("slice".equals(kind) ? 2 : 3);
    final ByteBuffer buffer;
    if ("slice".equals(kind)) {
      buffer = storage.slice();
      buffer.position(1);
    } else if ("readonly".equals(kind)) {
      buffer = storage.asReadOnlyBuffer();
    } else {
      buffer = storage;
    }
    buffer.mark();
    final int position = buffer.position();
    final int limit = buffer.limit();
    final InputEntityReader reader = new JsonInputFormat(null, null, null, false, false, nodes)
        .createReader(SCHEMA, new ByteEntity(buffer), null);
    assertRows(reader);
    assertRows(reader);
    int sampled = 0;
    try (final CloseableIterator<InputRowListPlusRawValues> rows = reader.sample()) {
      while (rows.hasNext()) {
        final InputRowListPlusRawValues row = rows.next();
        Assertions.assertNull(row.getParseException());
        sampled += row.getInputRows().size();
      }
    }
    Assertions.assertEquals(2, sampled);
    Assertions.assertEquals(position, buffer.position());
    Assertions.assertEquals(limit, buffer.limit());
    buffer.reset();
    Assertions.assertEquals(position, buffer.position());
  }

  @ParameterizedTest
  @CsvSource({"false,UTF-16LE", "true,UTF-16LE", "false,UTF-16BE", "true,UTF-16BE"})
  public void testEncodingDetection(final boolean nodes, final String encoding) throws IOException
  {
    final byte[] encoded = new String(DATA, StandardCharsets.UTF_8).getBytes(Charset.forName(encoding));
    final ByteBuffer buffer = ByteBuffer.allocate(encoded.length + 5);
    buffer.position(3);
    buffer.put(encoded);
    buffer.limit(buffer.position());
    buffer.position(3);
    assertRows(new JsonInputFormat(null, null, null, false, false, nodes)
                   .createReader(SCHEMA, new ByteEntity(buffer), null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCustomOpenIsRespected(final boolean nodes) throws IOException
  {
    final ByteEntity entity = new ByteEntity(StringUtils.toUtf8("invalid raw data"))
    {
      @Override
      public InputStream open()
      {
        return new ByteArrayInputStream(DATA);
      }
    };
    assertRows(new JsonInputFormat(null, null, null, false, false, nodes).createReader(SCHEMA, entity, null));
  }

  private static void assertRows(final InputEntityReader reader) throws IOException
  {
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      Assertions.assertEquals("café", rows.next().getRaw("key"));
      Assertions.assertEquals("東京", rows.next().getRaw("key"));
      Assertions.assertFalse(rows.hasNext());
    }
  }
}
