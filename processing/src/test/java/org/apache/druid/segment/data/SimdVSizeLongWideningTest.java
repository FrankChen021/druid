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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Runs the species dependent widening code of {@link SimdVSizeLongDeserializers} with every long species that
 * {@link SimdVSizeLongDeserializers#LONG_SPECIES} can be, not just the one picked for the machine running the test
 * (which {@link SimdVSizeLongDeserializersTest} covers end to end).
 */
public class SimdVSizeLongWideningTest
{
  private static final int NUM_VALUES = 64;
  private static final int OUT_POSITION = 3;
  private static final long SENTINEL = 0xDEADBEEFL;

  public static List<VectorSpecies<Long>> longSpecies()
  {
    return List.of(LongVector.SPECIES_64, LongVector.SPECIES_128, LongVector.SPECIES_256);
  }

  @Test
  public void testSpeciesCapped()
  {
    Assertions.assertTrue(SimdVSizeLongDeserializers.LONG_SPECIES.vectorBitSize() <= 256);
    Assertions.assertTrue(longSpecies().contains(SimdVSizeLongDeserializers.LONG_SPECIES));
  }

  @ParameterizedTest
  @MethodSource("longSpecies")
  public void testWidenDelta(VectorSpecies<Long> longSpecies)
  {
    final VectorSpecies<Byte> byteSpecies = longSpecies.vectorShape().withLanes(byte.class);
    for (int bytesPerValue : new int[]{Byte.BYTES, Short.BYTES}) {
      final byte[] source = randomBytes(bytesPerValue);
      final long base = -1234567;
      final long[] expected = newOut();
      for (int i = 0; i < NUM_VALUES; i++) {
        long value = 0;
        for (int b = 0; b < bytesPerValue; b++) {
          value = (value << 8) | (source[i * bytesPerValue + b] & 0xFF);
        }
        expected[OUT_POSITION + i] = base + value;
      }
      final long[] out = newOut();
      SimdVSizeLongDeserializers.widenDelta(
          longSpecies,
          byteSpecies,
          SimdVSizeLongDeserializers.widenShuffle(byteSpecies, bytesPerValue),
          SimdVSizeLongDeserializers.widenMask(byteSpecies, bytesPerValue),
          bytesPerValue,
          source,
          NUM_VALUES,
          out,
          OUT_POSITION,
          base
      );
      Assertions.assertArrayEquals(expected, out, longSpecies + " bytesPerValue " + bytesPerValue);
    }
  }

  @ParameterizedTest
  @MethodSource("longSpecies")
  public void testWidenTable(VectorSpecies<Long> longSpecies)
  {
    final VectorSpecies<Byte> byteSpecies = longSpecies.vectorShape().withLanes(byte.class);
    final VectorShuffle<Byte> shuffle = SimdVSizeLongDeserializers.widenShuffle(byteSpecies, Byte.BYTES);
    final ByteVector mask = SimdVSizeLongDeserializers.widenMask(byteSpecies, Byte.BYTES);
    final Random random = new Random(longSpecies.length());
    for (int bits : new int[]{1, 2}) {
      for (int tableSize = 1; tableSize <= (1 << bits); tableSize++) {
        final long[] table = random.longs(tableSize).toArray();
        final byte[] indexes = new byte[NUM_VALUES + ByteVector.SPECIES_256.length()];
        final long[] expected = newOut();
        for (int i = 0; i < NUM_VALUES; i++) {
          indexes[i] = (byte) random.nextInt(tableSize);
          expected[OUT_POSITION + i] = table[indexes[i]];
        }
        final long[] out = newOut();
        if (bits == 1) {
          SimdVSizeLongDeserializers.widenTable1(
              longSpecies,
              byteSpecies,
              shuffle,
              mask,
              indexes,
              NUM_VALUES,
              out,
              OUT_POSITION,
              table
          );
        } else {
          SimdVSizeLongDeserializers.widenTable2(
              longSpecies,
              byteSpecies,
              shuffle,
              mask,
              indexes,
              NUM_VALUES,
              out,
              OUT_POSITION,
              table
          );
        }
        Assertions.assertArrayEquals(expected, out, longSpecies + " bits " + bits + " tableSize " + tableSize);
      }
    }
  }

  private static byte[] randomBytes(int bytesPerValue)
  {
    final byte[] bytes = new byte[NUM_VALUES * bytesPerValue + ByteVector.SPECIES_256.length()];
    new Random(bytesPerValue).nextBytes(bytes);
    // include the extremes
    Arrays.fill(bytes, 0, bytesPerValue, (byte) 0xFF);
    Arrays.fill(bytes, bytesPerValue, 2 * bytesPerValue, (byte) 0);
    return bytes;
  }

  private static long[] newOut()
  {
    final long[] out = new long[OUT_POSITION + NUM_VALUES + 9];
    Arrays.fill(out, SENTINEL);
    return out;
  }
}
