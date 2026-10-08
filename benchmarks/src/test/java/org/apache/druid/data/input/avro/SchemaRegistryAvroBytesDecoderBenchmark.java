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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Measures schema-registry Avro decoding with one schema ID or alternating schema IDs.
 *
 * <p>The decoder is initialized once per trial, while each operation parses one complete schema-registry wire
 * message and consumes all fields in the returned {@link GenericRecord}.</p>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
public class SchemaRegistryAvroBytesDecoderBenchmark
{
  private static final String SUBJECT = "binary-streaming-value";
  private static final int WIRE_HEADER_SIZE = 1 + Integer.BYTES;
  private static final Schema FIRST_SCHEMA = new Schema.Parser().parse(
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
      + "}"
  );
  private static final Schema SECOND_SCHEMA = new Schema.Parser().parse(
      "{"
      + "\"type\":\"record\","
      + "\"name\":\"StreamingEventV2\","
      + "\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},"
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"value\",\"type\":\"long\"},"
      + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"NestedValueV2\","
      + "\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}},"
      + "{\"name\":\"payload\",\"type\":\"string\"}"
      + "]"
      + "}"
  );

  @Param({"same", "alternating"})
  private String schemaPattern;

  private SchemaRegistryBasedAvroBytesDecoder decoder;
  private byte[] firstMessage;
  private byte[] secondMessage;
  private int messageIndex;

  @Setup
  public void setUp() throws Exception
  {
    final MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
    final int firstSchemaId = registry.register(SUBJECT, new AvroSchema(FIRST_SCHEMA));
    final int secondSchemaId = registry.register(SUBJECT, new AvroSchema(SECOND_SCHEMA));
    decoder = new SchemaRegistryBasedAvroBytesDecoder(registry);
    firstMessage = createWireMessage(FIRST_SCHEMA, firstSchemaId, 256);
    secondMessage = createWireMessage(SECOND_SCHEMA, secondSchemaId, 256);
  }

  @Benchmark
  public void parseAndRead(final Blackhole blackhole)
  {
    final byte[] message;
    if ("same".equals(schemaPattern)) {
      message = firstMessage;
    } else {
      message = (messageIndex++ & 1) == 0 ? firstMessage : secondMessage;
    }
    final GenericRecord record = decoder.parse(ByteBuffer.wrap(message));
    blackhole.consume(record.get("timestamp"));
    blackhole.consume(record.get("id"));
    blackhole.consume(record.get("value"));
    final GenericRecord nested = (GenericRecord) record.get("nested");
    blackhole.consume(nested.get("label"));
    blackhole.consume(record.get("payload"));
  }

  private static byte[] createWireMessage(
      final Schema schema,
      final int schemaId,
      final int targetSize
  ) throws IOException
  {
    int payloadLength = Math.max(0, targetSize - WIRE_HEADER_SIZE - 64);
    byte[] wireMessage = new byte[0];
    for (int attempt = 0; attempt < 10; attempt++) {
      final GenericRecord record = new GenericData.Record(schema);
      record.put("timestamp", 1L);
      record.put("id", "streaming-id");
      record.put("value", 42L);
      final GenericRecord nested = new GenericData.Record(schema.getField("nested").schema());
      nested.put("label", "streaming-label");
      record.put("nested", nested);
      record.put("payload", "x".repeat(payloadLength));

      final ByteArrayOutputStream output = new ByteArrayOutputStream(targetSize);
      final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
      new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
      encoder.flush();
      final byte[] datum = output.toByteArray();
      wireMessage = ByteBuffer.allocate(WIRE_HEADER_SIZE + datum.length)
                               .put((byte) 0)
                               .putInt(schemaId)
                               .put(datum)
                               .array();

      final int delta = targetSize - wireMessage.length;
      if (delta == 0 || (delta < 0 && payloadLength == 0)) {
        break;
      }
      payloadLength = Math.max(0, payloadLength + delta);
    }
    return wireMessage;
  }

  public static void main(final String[] args) throws RunnerException
  {
    final Options options = new OptionsBuilder()
        .include(SchemaRegistryAvroBytesDecoderBenchmark.class.getSimpleName())
        .build();
    new Runner(options).run();
  }
}
