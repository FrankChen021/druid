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

package org.apache.druid.data.input.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

public class AvroStreamReaderBufferTest
{
  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}"
  );

  @ParameterizedTest
  @CsvSource({"inline,heap", "inline,slice", "inline,direct", "inline,readonly",
              "multiple,heap", "multiple,slice", "multiple,direct", "multiple,readonly",
              "registry,heap", "registry,slice", "registry,direct", "registry,readonly"})
  public void testBufferBoundsAndRepeatedReads(final String decoderType, final String kind) throws Exception
  {
    final byte[] payload = encode();
    final AvroBytesDecoder decoder;
    final byte[] bytes;
    if ("inline".equals(decoderType)) {
      decoder = new InlineSchemaAvroBytesDecoder(SCHEMA);
      bytes = payload;
    } else {
      bytes = ByteBuffer.allocate(payload.length + 5)
                        .put((byte) ("multiple".equals(decoderType) ? 1 : 0)).putInt(1).put(payload).array();
      if ("multiple".equals(decoderType)) {
        decoder = new InlineSchemasAvroBytesDecoder(Collections.singletonMap(1, SCHEMA));
      } else {
        final SchemaRegistryClient registry = Mockito.mock(SchemaRegistryClient.class);
        Mockito.when(registry.getSchemaById(1)).thenReturn(new AvroSchema(SCHEMA));
        decoder = new SchemaRegistryBasedAvroBytesDecoder(registry);
      }
    }
    final ByteBuffer storage = "direct".equals(kind)
                               ? ByteBuffer.allocateDirect(bytes.length + 7)
                               : ByteBuffer.allocate(bytes.length + 7);
    storage.position(3);
    storage.put(bytes);
    storage.limit(storage.position());
    storage.position("slice".equals(kind) ? 2 : 3);
    final ByteBuffer buffer;
    if ("slice".equals(kind)) {
      buffer = storage.slice();
      buffer.position(1);
    } else {
      buffer = "readonly".equals(kind) ? storage.asReadOnlyBuffer() : storage;
    }
    buffer.mark();
    final int position = buffer.position();
    final InputEntityReader reader = createReader(decoder, buffer);
    for (int i = 0; i < 2; i++) {
      try (final CloseableIterator<InputRow> rows = reader.read()) {
        Assertions.assertEquals("hello", rows.next().getRaw("value"));
        Assertions.assertFalse(rows.hasNext());
      }
    }
    buffer.reset();
    Assertions.assertEquals(position, buffer.position());
    final byte[] remaining = new byte[buffer.remaining()];
    buffer.duplicate().get(remaining);
    Assertions.assertArrayEquals(bytes, remaining);
  }

  @Test
  public void testCustomDecoderReceivesPrivateCopy() throws Exception
  {
    final byte[] bytes = encode();
    final byte[] original = bytes.clone();
    final GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("value", "hello");
    final AvroBytesDecoder decoder = buffer -> {
      buffer.put(0, (byte) 0);
      return record;
    };
    try (final CloseableIterator<InputRow> rows = createReader(decoder, ByteBuffer.wrap(bytes)).read()) {
      Assertions.assertEquals("hello", rows.next().getRaw("value"));
    }
    Assertions.assertArrayEquals(original, bytes);
  }

  private static InputEntityReader createReader(final AvroBytesDecoder decoder, final ByteBuffer buffer)
  {
    return new AvroStreamInputFormat(null, decoder, false, false).createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "auto", DateTimes.EPOCH),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        new ByteEntity(buffer),
        null
    );
  }

  private static byte[] encode() throws Exception
  {
    final GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("value", "hello");
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, encoder);
    encoder.flush();
    return output.toByteArray();
  }
}
