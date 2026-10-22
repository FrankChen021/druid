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

package org.apache.druid.benchmark.compression;

import org.apache.druid.segment.data.VSizeLongSerde;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the scalar and SIMD ({@code jdk.incubator.vector}) implementations of the contiguous bulk decoding methods
 * of {@link VSizeLongSerde.LongDeserializer}, which back vectorized reads of delta and table encoded long columns.
 * Each invocation decodes {@code vectorSize} values starting at the next of many random start offsets (most of them
 * not byte aligned) spread over a large encoded buffer. The {@code impl} param puts scalar and SIMD side by side in
 * the result table.
 * <p>
 * Build the benchmarks jar from the repository root:
 * <pre>
 * mvn package -pl benchmarks -am -DskipTests -Dmaven.install.skip=true -Pskip-static-checks -Dweb.console.skip=true -T1C
 * </pre>
 * Run everything (the fork adds {@code --add-modules=jdk.incubator.vector} itself):
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar VSizeLongDecodeBenchmark
 * </pre>
 * {@code getDelta} covers 1, 2, 4, 8 and 16 bits; {@code getTable} covers 1, 2, 4 and 8 bits (table encoding is limited
 * to 256 entries), of which only 1 and 2 bits have a SIMD implementation. To limit the matrix, e.g.
 * {@code java -jar benchmarks/target/benchmarks.jar 'VSizeLongDecodeBenchmark.getDelta' -p bufferType=direct
 * -p vectorSize=512}. Useful extras: {@code -prof perfasm} (Linux, needs hsdis) to check the generated vector
 * instructions, and {@code -jvmArgsAppend -XX:UseAVX=2} to compare AVX2 with AVX-512 on the same machine.
 */
@Fork(value = 1, jvmArgsAppend = "--add-modules=jdk.incubator.vector")
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VSizeLongDecodeBenchmark
{
  private static final int NUM_VALUES = 1 << 22;
  private static final int NUM_STARTS = 1 << 12;
  private static final int BUFFER_OFFSET = 7;

  @State(Scope.Benchmark)
  public abstract static class DecodeState
  {
    @Param({"scalar", "simd"})
    private String impl;

    @Param({"heap", "direct"})
    private String bufferType;

    @Param({"64", "512"})
    int vectorSize;

    VSizeLongSerde.LongDeserializer deserializer;
    long[] out;
    long[] table;
    private int[] starts;
    private int startIndex;

    abstract int bitsPerValue();

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
      final int bitsPerValue = bitsPerValue();
      final Random random = new Random(0xC0FFEEL);
      final long maxValue = (1L << bitsPerValue) - 1;

      final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      for (int i = 0; i < BUFFER_OFFSET; i++) {
        outStream.write(0);
      }
      try (VSizeLongSerde.LongSerializer serializer = VSizeLongSerde.getSerializer(bitsPerValue, outStream)) {
        for (int i = 0; i < NUM_VALUES; i++) {
          serializer.write(random.nextLong() & maxValue);
        }
      }
      final byte[] bytes = outStream.toByteArray();
      final ByteBuffer buffer;
      switch (bufferType) {
        case "heap":
          buffer = ByteBuffer.wrap(bytes);
          break;
        case "direct":
          buffer = ByteBuffer.allocateDirect(bytes.length);
          buffer.put(bytes).flip();
          break;
        default:
          throw new IllegalArgumentException("Unknown bufferType " + bufferType);
      }

      final boolean useVectorApi;
      switch (impl) {
        case "scalar":
          useVectorApi = false;
          break;
        case "simd":
          useVectorApi = true;
          break;
        default:
          throw new IllegalArgumentException("Unknown impl " + impl);
      }
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, BUFFER_OFFSET, useVectorApi);

      starts = new int[NUM_STARTS];
      for (int i = 0; i < NUM_STARTS; i++) {
        starts[i] = random.nextInt(NUM_VALUES - vectorSize);
      }
      out = new long[vectorSize];
      table = new long[(int) Math.min(maxValue + 1, 256)];
      for (int i = 0; i < table.length; i++) {
        table[i] = random.nextLong();
      }
    }

    int nextStart()
    {
      final int start = starts[startIndex];
      startIndex = (startIndex + 1) & (NUM_STARTS - 1);
      return start;
    }
  }

  @State(Scope.Benchmark)
  public static class DeltaState extends DecodeState
  {
    @Param({"1", "2", "4", "8", "16"})
    private int bitsPerValue;

    @Override
    int bitsPerValue()
    {
      return bitsPerValue;
    }
  }

  @State(Scope.Benchmark)
  public static class TableState extends DecodeState
  {
    @Param({"1", "2", "4", "8"})
    private int bitsPerValue;

    @Override
    int bitsPerValue()
    {
      return bitsPerValue;
    }
  }

  @Benchmark
  public void getDelta(final DeltaState state, final Blackhole blackhole)
  {
    state.deserializer.getDelta(state.out, 0, state.nextStart(), state.vectorSize, 1_000_000_007L);
    blackhole.consume(state.out);
  }

  @Benchmark
  public void getTable(final TableState state, final Blackhole blackhole)
  {
    state.deserializer.getTable(state.out, 0, state.nextStart(), state.vectorSize, state.table);
    blackhole.consume(state.out);
  }
}
