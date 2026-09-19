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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
import org.apache.druid.data.input.avro.AvroStreamInputFormat;
import org.apache.druid.data.input.avro.InlineSchemaAvroBytesDecoder;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.protobuf.InlineDescriptorProtobufBytesDecoder;
import org.apache.druid.data.input.protobuf.ProtobufInputFormat;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures one-message streaming Avro and Protobuf parsing with a reader reused between records.
 *
 * <p>Each operation installs one new {@link ByteEntity} in a {@link SettableByteEntity}, reads one row through the
 * selected built-in input format, and consumes every discovered field.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class BinaryInputFormatStreamingBenchmark
{
  private static final String AVRO_SCHEMA_JSON =
      "{"
      + "\"type\":\"record\","
      + "\"name\":\"StreamingEvent\","
      + "\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},"
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"value\",\"type\":\"long\"},"
      + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"NestedValue\","
      + "\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}},"
      + "{\"name\":\"payload\",\"type\":\"string\"}"
      + "]"
      + "}";
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
  private static final Map<String, Object> AVRO_SCHEMA_MAP = parseAvroSchema();
  private static final String PROTO_DESCRIPTOR = createProtoDescriptor();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(
      "__binary_auto_timestamp",
      "auto",
      DateTimes.EPOCH
  );
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      TIMESTAMP_SPEC,
      DimensionsSpec.builder().useSchemaDiscovery(true).build(),
      ColumnsFilter.all()
  );

  @Param({"avro", "protobuf"})
  private String formatName;

  @Param({"256", "4096"})
  private int targetPayloadSize;

  private SettableByteEntity<ByteEntity> source;
  private InputEntityReader reader;
  private byte[] messageBytes;

  @Setup
  public void setUp() throws Exception
  {
    source = new SettableByteEntity<>();
    if ("avro".equals(formatName)) {
      final InlineSchemaAvroBytesDecoder decoder = new InlineSchemaAvroBytesDecoder(
          new ObjectMapper(),
          AVRO_SCHEMA_MAP
      );
      final AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(null, decoder, false, false);
      messageBytes = createAvroMessage(targetPayloadSize);
      reader = inputFormat.createReader(INPUT_ROW_SCHEMA, source, null);
    } else if ("protobuf".equals(formatName)) {
      final InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(
          PROTO_DESCRIPTOR,
          "StreamingEvent"
      );
      final ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, decoder);
      messageBytes = createProtobufMessage(targetPayloadSize, decoder.getDescriptor());
      reader = inputFormat.createReader(INPUT_ROW_SCHEMA, source, null);
    } else {
      throw new IllegalArgumentException("Unknown format: " + formatName);
    }
  }

  @Benchmark
  public void parseAndRead(final Blackhole blackhole) throws IOException
  {
    source.setEntity(new ByteEntity(messageBytes));
    int rowCount = 0;
    try (final CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        rowCount++;
        blackhole.consume(row.getTimestampFromEpoch());
        for (final String dimension : row.getDimensions()) {
          blackhole.consume(dimension);
          blackhole.consume(row.getRaw(dimension));
        }
      }
    }

    if (rowCount != 1) {
      throw new IllegalStateException("Expected one row, got " + rowCount);
    }
  }

  private static Map<String, Object> parseAvroSchema()
  {
    try {
      return new ObjectMapper().readValue(AVRO_SCHEMA_JSON, new TypeReference<>() {});
    }
    catch (IOException e) {
      throw new IllegalStateException("Unable to parse Avro schema", e);
    }
  }

  private static byte[] createAvroMessage(final int targetSize) throws IOException
  {
    int payloadLength = Math.max(0, targetSize - 64);
    byte[] messageBytes = new byte[0];
    for (int attempt = 0; attempt < 10; attempt++) {
      final GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
      record.put("timestamp", 1L);
      record.put("id", "streaming-id");
      record.put("value", 42L);
      final GenericRecord nested = new GenericData.Record(AVRO_SCHEMA.getField("nested").schema());
      nested.put("label", "streaming-label");
      record.put("nested", nested);
      record.put("payload", "x".repeat(payloadLength));

      final ByteArrayOutputStream output = new ByteArrayOutputStream(targetSize);
      final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
      new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA).write(record, encoder);
      encoder.flush();
      messageBytes = output.toByteArray();

      final int delta = targetSize - messageBytes.length;
      if (delta == 0 || (delta < 0 && payloadLength == 0)) {
        break;
      }
      payloadLength = Math.max(0, payloadLength + delta);
    }
    return messageBytes;
  }

  private static byte[] createProtobufMessage(
      final int targetSize,
      final Descriptors.Descriptor descriptor
  )
  {
    int payloadLength = Math.max(0, targetSize - 64);
    byte[] messageBytes = new byte[0];
    final Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName("timestamp");
    final Descriptors.FieldDescriptor idField = descriptor.findFieldByName("id");
    final Descriptors.FieldDescriptor valueField = descriptor.findFieldByName("value");
    final Descriptors.FieldDescriptor nestedField = descriptor.findFieldByName("nested");
    final Descriptors.FieldDescriptor payloadField = descriptor.findFieldByName("payload");
    final Descriptors.FieldDescriptor labelField = nestedField.getMessageType().findFieldByName("label");

    for (int attempt = 0; attempt < 10; attempt++) {
      final DynamicMessage nested = DynamicMessage.newBuilder(nestedField.getMessageType())
                                                   .setField(labelField, "streaming-label")
                                                   .build();
      final DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor)
                                                            .setField(timestampField, 1L)
                                                            .setField(idField, "streaming-id")
                                                            .setField(valueField, 42L)
                                                            .setField(nestedField, nested)
                                                            .setField(payloadField, "x".repeat(payloadLength));
      messageBytes = builder.build().toByteArray();

      final int delta = targetSize - messageBytes.length;
      if (delta == 0 || (delta < 0 && payloadLength == 0)) {
        break;
      }
      payloadLength = Math.max(0, payloadLength + delta);
    }
    return messageBytes;
  }

  private static String createProtoDescriptor()
  {
    final DescriptorProtos.FieldDescriptorProto labelField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                                                                                                    .setName("label")
                                                                                                    .setNumber(1)
                                                                                                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                                                                                    .build();
    final DescriptorProtos.DescriptorProto nested = DescriptorProtos.DescriptorProto.newBuilder()
                                                                                      .setName("NestedValue")
                                                                                      .addField(labelField)
                                                                                      .build();
    final DescriptorProtos.FieldDescriptorProto timestampField = createProtoField(
        "timestamp",
        1,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
    );
    final DescriptorProtos.FieldDescriptorProto idField = createProtoField(
        "id",
        2,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
    );
    final DescriptorProtos.FieldDescriptorProto valueField = createProtoField(
        "value",
        3,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
    );
    final DescriptorProtos.FieldDescriptorProto nestedField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                                                                                                     .setName("nested")
                                                                                                     .setNumber(4)
                                                                                                     .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                                                                                     .setTypeName(".StreamingEvent.NestedValue")
                                                                                                     .build();
    final DescriptorProtos.FieldDescriptorProto payloadField = createProtoField(
        "payload",
        5,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
    );
    final DescriptorProtos.DescriptorProto event = DescriptorProtos.DescriptorProto.newBuilder()
                                                                                    .setName("StreamingEvent")
                                                                                    .addNestedType(nested)
                                                                                    .addField(timestampField)
                                                                                    .addField(idField)
                                                                                    .addField(valueField)
                                                                                    .addField(nestedField)
                                                                                    .addField(payloadField)
                                                                                    .build();
    final DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                                                                                           .setName("binary_streaming.proto")
                                                                                           .setSyntax("proto3")
                                                                                           .addMessageType(event)
                                                                                           .build();
    return Base64.getEncoder().encodeToString(
        DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file).build().toByteArray()
    );
  }

  private static DescriptorProtos.FieldDescriptorProto createProtoField(
      final String name,
      final int number,
      final DescriptorProtos.FieldDescriptorProto.Type type
  )
  {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
                                                 .setName(name)
                                                 .setNumber(number)
                                                 .setType(type)
                                                 .build();
  }

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(BinaryInputFormatStreamingBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }
}
