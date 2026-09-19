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

package org.apache.druid.data.input.protobuf;

import com.google.common.collect.Lists;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

public class ProtobufReaderTest
{
  private InputRowSchema inputRowSchema;
  private InputRowSchema inputRowSchemaWithComplexTimestamp;
  private JSONPathSpec flattenSpec;
  private FileBasedProtobufBytesDecoder decoder;

  @BeforeEach
  public void setUp()
  {
    TimestampSpec timestampSpec = new TimestampSpec("timestamp", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Lists.newArrayList(
            new StringDimensionSchema("event"),
            new StringDimensionSchema("id"),
            new StringDimensionSchema("someOtherId"),
            new StringDimensionSchema("isValid")
        )
    );
    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "foobar", "$.foo.bar"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar0", "$.bar[0].bar")
        )
    );

    inputRowSchema = new InputRowSchema(timestampSpec, dimensionsSpec, null);
    inputRowSchemaWithComplexTimestamp = new InputRowSchema(
        new TimestampSpec("otherTimestamp", "iso", null),
        dimensionsSpec,
        null
    );
    decoder = new FileBasedProtobufBytesDecoder("proto_test_event.desc", "ProtoTestEvent");
  }

  @Test
  public void testParseNestedData() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchema, null, decoder, flattenSpec);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputFormatTest.buildNestedData(dateTime);

    ByteBuffer buffer = ProtobufInputFormatTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputFormatTest.verifyNestedData(row, dateTime);
  }

  @ParameterizedTest
  @ValueSource(strings = {"heap", "slice", "direct", "readonly"})
  public void testBufferBoundsAndRepeatedReads(final String kind) throws Exception
  {
    final DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    final byte[] bytes = ProtobufInputFormatTest.buildFlatData(dateTime).toByteArray();
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
    final ProtobufReader reader = new ProtobufReader(inputRowSchema, new ByteEntity(buffer), decoder, flattenSpec);
    for (int i = 0; i < 2; i++) {
      try (final CloseableIterator<InputRow> rows = reader.read()) {
        ProtobufInputFormatTest.verifyFlatData(rows.next(), dateTime, false);
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
    final DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    final byte[] bytes = ProtobufInputFormatTest.buildFlatData(dateTime).toByteArray();
    final byte[] original = bytes.clone();
    final DynamicMessage message = decoder.parse(ByteBuffer.wrap(bytes));
    final ProtobufBytesDecoder customDecoder = buffer -> {
      buffer.put(0, (byte) 0);
      return message;
    };
    final ProtobufReader reader =
        new ProtobufReader(inputRowSchema, new ByteEntity(bytes), customDecoder, flattenSpec);
    try (final CloseableIterator<InputRow> rows = reader.read()) {
      ProtobufInputFormatTest.verifyFlatData(rows.next(), dateTime, false);
    }
    Assertions.assertArrayEquals(original, bytes);
  }

  @Test
  public void testParseFlatData() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchema, null, decoder, null);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputFormatTest.buildFlatData(dateTime);

    ByteBuffer buffer = ProtobufInputFormatTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputFormatTest.verifyFlatData(row, dateTime, false);
  }

  @Test
  public void testParseFlatDataWithComplexTimestamp() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchemaWithComplexTimestamp, null, decoder, null);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputFormatTest.buildFlatDataWithComplexTimestamp(dateTime);

    ByteBuffer buffer = ProtobufInputFormatTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputFormatTest.verifyFlatDataWithComplexTimestamp(row, dateTime, false);
  }

  @Test
  public void testParseFlatDataWithComplexTimestampWithDefaultFlattenSpec() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(
        inputRowSchemaWithComplexTimestamp,
        null,
        decoder,
        JSONPathSpec.DEFAULT
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputFormatTest.buildFlatDataWithComplexTimestamp(dateTime);

    ByteBuffer buffer = ProtobufInputFormatTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputFormatTest.verifyFlatDataWithComplexTimestamp(row, dateTime, false);
  }
}
