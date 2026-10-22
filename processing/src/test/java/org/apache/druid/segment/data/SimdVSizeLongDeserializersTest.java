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

package org.apache.druid.segment.data;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Compares the SIMD deserializers in {@link SimdVSizeLongDeserializers} against the scalar ones and against the
 * original values, for many combinations of start offset, length, output position, base and buffer type.
 */
@ParameterizedClass
@MethodSource("data")
public class SimdVSizeLongDeserializersTest
{
  private static final int NUM_VALUES = 2000;
  private static final int BUFFER_OFFSET = 3;
  private static final long SENTINEL = 0xDEADBEEFL;
  private static final int[] LENGTHS = {0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 23, 31, 32, 33, 64, 100, 511, 512, 513};
  private static final int[] OUT_POSITIONS = {0, 3};
  private static final long[] BASES = {0, 1, -1234567, Long.MAX_VALUE / 3, Long.MIN_VALUE + 5};

  private final int numBits;
  private final String bufferType;
  private final long[] values;
  private final VSizeLongSerde.LongDeserializer scalar;
  private final VSizeLongSerde.LongDeserializer simd;

  public SimdVSizeLongDeserializersTest(int numBits, String bufferType) throws IOException
  {
    this.numBits = numBits;
    this.bufferType = bufferType;
    final Random random = new Random(numBits * 31L + bufferType.hashCode());
    this.values = new long[NUM_VALUES];
    final long maxValue = (1L << numBits) - 1;
    for (int i = 0; i < NUM_VALUES; i++) {
      // mix of random values and the extremes
      final int choice = random.nextInt(10);
      values[i] = choice == 0 ? 0 : choice == 1 ? maxValue : random.nextLong() & maxValue;
    }
    final ByteBuffer buffer = serialize(numBits, values, bufferType);
    this.scalar = VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET, false);
    this.simd = VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET, true);
  }

  public static List<Object[]> data()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (int numBits : new int[]{1, 2, 4, 8, 16}) {
      for (String bufferType : new String[]{"heap", "heapSlice", "heapReadOnly", "direct"}) {
        constructors.add(new Object[]{numBits, bufferType});
      }
    }
    return constructors;
  }

  private static ByteBuffer serialize(int numBits, long[] values, String bufferType) throws IOException
  {
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    for (int i = 0; i < BUFFER_OFFSET; i++) {
      outStream.write(0xAF);
    }
    try (VSizeLongSerde.LongSerializer serializer = VSizeLongSerde.getSerializer(numBits, outStream)) {
      for (long value : values) {
        serializer.write(value);
      }
    }
    final byte[] bytes = outStream.toByteArray();
    switch (bufferType) {
      case "heap":
        return ByteBuffer.wrap(bytes);
      case "heapSlice": {
        // non-zero arrayOffset
        final byte[] padded = new byte[bytes.length + 5];
        System.arraycopy(bytes, 0, padded, 5, bytes.length);
        final ByteBuffer slice = ByteBuffer.wrap(padded, 5, bytes.length).slice();
        Assertions.assertEquals(5, slice.arrayOffset());
        return slice;
      }
      case "heapReadOnly":
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
      case "direct": {
        final ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes).flip();
        return direct;
      }
      default:
        throw new IllegalArgumentException(bufferType);
    }
  }

  @Test
  public void testSimdImplementationSelected()
  {
    Assertions.assertTrue(
        simd.getClass().getName().contains("Simd"),
        StringUtils.format("Deserializer class [%s]", simd.getClass().getName())
    );
    Assertions.assertFalse(scalar.getClass().getName().contains("Simd"));
  }

  @Test
  public void testGet()
  {
    for (int i = 0; i < NUM_VALUES; i++) {
      Assertions.assertEquals(values[i], simd.get(i));
      Assertions.assertEquals(values[i], scalar.get(i));
    }
  }

  @Test
  public void testGetDelta()
  {
    for (int start = 0; start < 40; start++) {
      checkDelta(start);
    }
    final Random random = new Random(1);
    for (int n = 0; n < 20; n++) {
      checkDelta(random.nextInt(NUM_VALUES));
    }
    checkDelta(NUM_VALUES - 1);
  }

  private void checkDelta(int start)
  {
    final List<Integer> lengths = new ArrayList<>();
    for (int length : LENGTHS) {
      if (start + length <= NUM_VALUES) {
        lengths.add(length);
      }
    }
    lengths.add(NUM_VALUES - start);
    for (int length : lengths) {
      for (int outPosition : OUT_POSITIONS) {
        for (long base : BASES) {
          final long[] expected = newOut(outPosition, length);
          for (int i = 0; i < length; i++) {
            expected[outPosition + i] = base + values[start + i];
          }
          final long[] scalarOut = newOut(outPosition, length);
          scalar.getDelta(scalarOut, outPosition, start, length, base);
          final long[] simdOut = newOut(outPosition, length);
          simd.getDelta(simdOut, outPosition, start, length, base);
          final String message = StringUtils.format(
              "numBits[%d] bufferType[%s] start[%d] length[%d] outPosition[%d] base[%d]",
              numBits,
              bufferType,
              start,
              length,
              outPosition,
              base
          );
          Assertions.assertArrayEquals(expected, scalarOut, "scalar " + message);
          Assertions.assertArrayEquals(expected, simdOut, "simd " + message);
        }
      }
    }
  }

  @Test
  public void testGetTable()
  {
    if (numBits > 8) {
      // table encoding is limited to 256 entries
      return;
    }
    final Random random = new Random(2);
    // table sizes smaller than, equal to and larger than 2^numBits (the latter takes the scalar fallback)
    final int maxTableSize = 1 << numBits;
    for (int tableSize : new int[]{maxTableSize / 2 + 1, maxTableSize, maxTableSize + 3}) {
      final long[] table = new long[tableSize];
      for (int i = 0; i < tableSize; i++) {
        table[i] = random.nextLong();
      }
      table[0] = Long.MIN_VALUE;
      // only use indexes which are present in the table
      final long[] indexes = Arrays.stream(values).map(v -> v % Math.min(tableSize, maxTableSize)).toArray();
      final ByteBuffer buffer;
      try {
        buffer = serialize(numBits, indexes, bufferType);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      final VSizeLongSerde.LongDeserializer tableScalar =
          VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET, false);
      final VSizeLongSerde.LongDeserializer tableSimd =
          VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET, true);

      for (int start = 0; start < 40; start++) {
        for (int length : LENGTHS) {
          for (int outPosition : OUT_POSITIONS) {
            final long[] expected = newOut(outPosition, length);
            for (int i = 0; i < length; i++) {
              expected[outPosition + i] = table[(int) indexes[start + i]];
            }
            final long[] scalarOut = newOut(outPosition, length);
            tableScalar.getTable(scalarOut, outPosition, start, length, table);
            final long[] simdOut = newOut(outPosition, length);
            tableSimd.getTable(simdOut, outPosition, start, length, table);
            final String message = StringUtils.format(
                "numBits[%d] bufferType[%s] tableSize[%d] start[%d] length[%d] outPosition[%d]",
                numBits,
                bufferType,
                tableSize,
                start,
                length,
                outPosition
            );
            Assertions.assertArrayEquals(expected, scalarOut, "scalar " + message);
            Assertions.assertArrayEquals(expected, simdOut, "simd " + message);
          }
        }
      }
    }
  }

  @Test
  public void testDispatchFollowsUseVectorApi() throws IOException
  {
    final ByteBuffer buffer = serialize(numBits, values, bufferType);
    try {
      ExpressionProcessing.initializeForVectorApiTests();
      Assertions.assertEquals(
          simd.getClass(),
          VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET).getClass()
      );
      ExpressionProcessing.initializeForTests();
      Assertions.assertEquals(
          scalar.getClass(),
          VSizeLongSerde.getDeserializer(numBits, buffer, BUFFER_OFFSET).getClass()
      );
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  private static long[] newOut(int outPosition, int length)
  {
    // trailing sentinels catch writes past the requested range
    final long[] out = new long[outPosition + length + 9];
    Arrays.fill(out, SENTINEL);
    return out;
  }
}
