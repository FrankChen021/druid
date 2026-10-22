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
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

import java.nio.ByteBuffer;

/**
 * SIMD ({@code jdk.incubator.vector}) specializations of the {@link VSizeLongSerde.LongDeserializer} implementations
 * for the byte friendly bit widths 1, 2, 4, 8 and 16. Selected by {@link VSizeLongSerde#getDeserializer} when
 * {@link org.apache.druid.math.expr.ExpressionProcessing#useVectorApi()} is enabled.
 * <p>
 * Only the contiguous {@code getDelta} methods (and {@code getTable} for 1 and 2 bits) are vectorized; single value
 * {@code get} and the {@code int[] indexes} variants are inherited from the scalar implementations, as are the
 * unaligned head and the remainder tail of each call. The aligned middle is decoded in two passes:
 * <ol>
 *   <li>The packed bytes are bulk copied out of the {@link ByteBuffer} into a scratch array (the Vector API has no
 *   {@link ByteBuffer} loads that work on JDK 21 without preview features). Sub-byte values are then unpacked, 8 at a
 *   time, into one byte per value with a byte shuffle, per lane shift and mask.</li>
 *   <li>The bytes are widened to longs. Rather than {@code convertShape(ZERO_EXTEND_B2L, ...)}, whose lane count
 *   changing forms are not intrinsified on all platforms (on aarch64 they are ~50x slower than scalar code), this uses
 *   a byte shuffle which moves each value's byte(s) into the low byte(s) of a long lane, masks off the rest of the lane
 *   and reinterprets the bytes as longs. The same shuffle also swaps the big endian bytes of 16 bit values.</li>
 * </ol>
 * Like the scalar deserializers (and the column readers which own them), instances are not thread-safe: the scratch
 * arrays are per instance.
 */
final class SimdVSizeLongDeserializers
{
  private static final int VALUES_PER_GROUP = 8;

  /**
   * Long species that values are widened to. Capped at 8 lanes so that a group of 8 values always fills a whole
   * number of long vectors.
   */
  static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED.length() > VALUES_PER_GROUP
                                                  ? LongVector.SPECIES_512
                                                  : LongVector.SPECIES_PREFERRED;
  private static final int LONG_LANES = LONG_SPECIES.length();

  /**
   * Byte species with the same shape as {@link #LONG_SPECIES}, used to widen bytes to longs.
   */
  private static final VectorSpecies<Byte> WIDE_BYTE_SPECIES = LONG_SPECIES.vectorShape().withLanes(byte.class);

  /**
   * 8 lane byte species used to unpack one group of 8 sub-byte values.
   */
  private static final VectorSpecies<Byte> GROUP_BYTE_SPECIES = ByteVector.SPECIES_64;

  /**
   * Vector loads read up to this many bytes past the last byte they use, so scratch arrays have this much slack.
   */
  private static final int SCRATCH_PADDING = Math.max(WIDE_BYTE_SPECIES.length(), GROUP_BYTE_SPECIES.length());

  /**
   * Whether the least significant byte of a long lane comes first when reinterpreting bytes as longs.
   */
  private static final boolean LEAST_SIGNIFICANT_BYTE_FIRST =
      ByteVector.zero(WIDE_BYTE_SPECIES).withLane(0, (byte) 1).reinterpretAsLongs().lane(0) == 1;

  private static final VectorShuffle<Byte> WIDEN_SHUFFLE_8 = widenShuffle(Byte.BYTES);
  private static final ByteVector WIDEN_MASK_8 = widenMask(Byte.BYTES);
  private static final VectorShuffle<Byte> WIDEN_SHUFFLE_16 = widenShuffle(Short.BYTES);
  private static final ByteVector WIDEN_MASK_16 = widenMask(Short.BYTES);

  // lane shuffles which copy the packed byte holding value i of a group into lane i
  private static final VectorShuffle<Byte> SPREAD_1 =
      VectorShuffle.fromValues(GROUP_BYTE_SPECIES, 0, 0, 0, 0, 0, 0, 0, 0);
  private static final VectorShuffle<Byte> SPREAD_2 =
      VectorShuffle.fromValues(GROUP_BYTE_SPECIES, 0, 0, 0, 0, 1, 1, 1, 1);
  private static final VectorShuffle<Byte> SPREAD_4 =
      VectorShuffle.fromValues(GROUP_BYTE_SPECIES, 0, 0, 1, 1, 2, 2, 3, 3);

  // right shift of value i of a group within its byte; values are packed most significant bits first
  private static final ByteVector SHIFTS_1 = groupVector(7, 6, 5, 4, 3, 2, 1, 0);
  private static final ByteVector SHIFTS_2 = groupVector(6, 4, 2, 0, 6, 4, 2, 0);
  private static final ByteVector SHIFTS_4 = groupVector(4, 0, 4, 0, 4, 0, 4, 0);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private SimdVSizeLongDeserializers()
  {
  }

  private static ByteVector groupVector(int... values)
  {
    final byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return ByteVector.fromArray(GROUP_BYTE_SPECIES, bytes, 0);
  }

  /**
   * Byte shuffle which, for long lane {@code j}, moves the big endian {@code bytesPerValue} byte value {@code j} of the
   * source into the low bytes of the lane. Lane bytes which are masked off by {@link #widenMask} take source byte 0.
   */
  private static VectorShuffle<Byte> widenShuffle(int bytesPerValue)
  {
    final int[] sourceIndexes = new int[WIDE_BYTE_SPECIES.length()];
    for (int lane = 0; lane < LONG_LANES; lane++) {
      for (int significance = 0; significance < bytesPerValue; significance++) {
        sourceIndexes[laneByteIndex(lane, significance)] = lane * bytesPerValue + bytesPerValue - 1 - significance;
      }
    }
    return VectorShuffle.fromArray(WIDE_BYTE_SPECIES, sourceIndexes, 0);
  }

  private static ByteVector widenMask(int bytesPerValue)
  {
    final byte[] mask = new byte[WIDE_BYTE_SPECIES.length()];
    for (int lane = 0; lane < LONG_LANES; lane++) {
      for (int significance = 0; significance < bytesPerValue; significance++) {
        mask[laneByteIndex(lane, significance)] = (byte) 0xFF;
      }
    }
    return ByteVector.fromArray(WIDE_BYTE_SPECIES, mask, 0);
  }

  private static int laneByteIndex(int lane, int significance)
  {
    return lane * Long.BYTES + (LEAST_SIGNIFICANT_BYTE_FIRST ? significance : Long.BYTES - 1 - significance);
  }

  /**
   * Widen {@link #LONG_LANES} values starting at byte {@code sourceOffset} of {@code source} to longs.
   */
  private static LongVector widen(byte[] source, int sourceOffset, VectorShuffle<Byte> shuffle, ByteVector mask)
  {
    return ByteVector.fromArray(WIDE_BYTE_SPECIES, source, sourceOffset)
                     .rearrange(shuffle)
                     .and(mask)
                     .reinterpretAsLongs();
  }

  /**
   * Write {@code base} plus each of the first {@code numValues} unsigned bytes of {@code values} to {@code out}.
   * {@code numValues} must be a multiple of {@link #VALUES_PER_GROUP}.
   */
  private static void widenDelta(byte[] values, int numValues, long[] out, int outPosition, long base)
  {
    for (int i = 0; i < numValues; i += LONG_LANES) {
      widen(values, i, WIDEN_SHUFFLE_8, WIDEN_MASK_8).add(base).intoArray(out, outPosition + i);
    }
  }

  /**
   * Table lookup of 1 bit indexes, branch free: {@code t0 ^ (-index & (t0 ^ t1))}
   */
  private static void widenTable1(byte[] indexes, int numValues, long[] out, int outPosition, long[] table)
  {
    final long t0 = tableValue(table, 0);
    final LongVector t0Vector = LongVector.broadcast(LONG_SPECIES, t0);
    final LongVector t0XorT1 = LongVector.broadcast(LONG_SPECIES, t0 ^ tableValue(table, 1));
    for (int i = 0; i < numValues; i += LONG_LANES) {
      final LongVector index = widen(indexes, i, WIDEN_SHUFFLE_8, WIDEN_MASK_8);
      t0Vector.lanewise(VectorOperators.XOR, index.neg().and(t0XorT1)).intoArray(out, outPosition + i);
    }
  }

  /**
   * Table lookup of 2 bit indexes, as two levels of the branch free select used by {@link #widenTable1}.
   */
  private static void widenTable2(byte[] indexes, int numValues, long[] out, int outPosition, long[] table)
  {
    final long t0 = tableValue(table, 0);
    final long t2 = tableValue(table, 2);
    final LongVector t0Vector = LongVector.broadcast(LONG_SPECIES, t0);
    final LongVector t0XorT1 = LongVector.broadcast(LONG_SPECIES, t0 ^ tableValue(table, 1));
    final LongVector t2Vector = LongVector.broadcast(LONG_SPECIES, t2);
    final LongVector t2XorT3 = LongVector.broadcast(LONG_SPECIES, t2 ^ tableValue(table, 3));
    for (int i = 0; i < numValues; i += LONG_LANES) {
      final LongVector index = widen(indexes, i, WIDEN_SHUFFLE_8, WIDEN_MASK_8);
      final LongVector lowMask = index.and(1).neg();
      final LongVector highMask = index.lanewise(VectorOperators.LSHR, 1).neg();
      final LongVector low = t0Vector.lanewise(VectorOperators.XOR, lowMask.and(t0XorT1));
      final LongVector high = t2Vector.lanewise(VectorOperators.XOR, lowMask.and(t2XorT3));
      low.lanewise(VectorOperators.XOR, highMask.and(low.lanewise(VectorOperators.XOR, high)))
         .intoArray(out, outPosition + i);
    }
  }

  private static long tableValue(long[] table, int index)
  {
    return index < table.length ? table[index] : 0;
  }

  /**
   * Per deserializer scratch arrays.
   */
  private static final class Scratch
  {
    private byte[] packed = EMPTY_BYTES;
    private byte[] unpacked = EMPTY_BYTES;

    private static byte[] ensureCapacity(byte[] array, int length)
    {
      if (array.length < length + SCRATCH_PADDING) {
        return new byte[Math.max(length + SCRATCH_PADDING, 2 * array.length)];
      }
      return array;
    }

    /**
     * Bulk copy {@code numBytes} bytes starting at absolute position {@code byteIndex} of {@code buffer} into the
     * packed scratch array, which is returned.
     */
    byte[] copy(ByteBuffer buffer, int byteIndex, int numBytes)
    {
      packed = ensureCapacity(packed, numBytes);
      buffer.get(byteIndex, packed, 0, numBytes);
      return packed;
    }

    /**
     * Copy and unpack {@code numGroups} groups of 8 {@code bitsPerValue} bit values, starting at absolute position
     * {@code byteIndex} of {@code buffer}, into an array of one byte per value, which is returned.
     */
    byte[] unpack(
        ByteBuffer buffer,
        int byteIndex,
        int numGroups,
        int bitsPerValue,
        VectorShuffle<Byte> spread,
        ByteVector shifts,
        byte mask
    )
    {
      final byte[] source = copy(buffer, byteIndex, numGroups * bitsPerValue);
      unpacked = ensureCapacity(unpacked, numGroups * VALUES_PER_GROUP);
      final byte[] target = unpacked;
      for (int group = 0; group < numGroups; group++) {
        ByteVector.fromArray(GROUP_BYTE_SPECIES, source, group * bitsPerValue)
                  .rearrange(spread)
                  .lanewise(VectorOperators.LSHR, shifts)
                  .and(mask)
                  .intoArray(target, group * VALUES_PER_GROUP);
      }
      return target;
    }
  }

  static final class SimdSize1Des extends VSizeLongSerde.Size1Des
  {
    private final Scratch scratch = new Scratch();

    SimdSize1Des(ByteBuffer buffer, int bufferOffset)
    {
      super(buffer, bufferOffset);
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x7) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++);
      }
      final int numGroups = (length - i) / VALUES_PER_GROUP;
      if (numGroups > 0) {
        final int numValues = numGroups * VALUES_PER_GROUP;
        final byte[] values = scratch.unpack(buffer, offset + (index >> 3), numGroups, 1, SPREAD_1, SHIFTS_1, (byte) 1);
        widenDelta(values, numValues, out, outPosition + i, base);
        i += numValues;
        index += numValues;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      if (table.length > 2) {
        super.getTable(out, outPosition, startIndex, length, table);
        return;
      }
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x7) != 0 && i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
      final int numGroups = (length - i) / VALUES_PER_GROUP;
      if (numGroups > 0) {
        final int numValues = numGroups * VALUES_PER_GROUP;
        final byte[] values = scratch.unpack(buffer, offset + (index >> 3), numGroups, 1, SPREAD_1, SHIFTS_1, (byte) 1);
        widenTable1(values, numValues, out, outPosition + i, table);
        i += numValues;
        index += numValues;
      }
      while (i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
    }
  }

  static final class SimdSize2Des extends VSizeLongSerde.Size2Des
  {
    private final Scratch scratch = new Scratch();

    SimdSize2Des(ByteBuffer buffer, int bufferOffset)
    {
      super(buffer, bufferOffset);
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x3) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++);
      }
      final int numGroups = (length - i) / VALUES_PER_GROUP;
      if (numGroups > 0) {
        final int numValues = numGroups * VALUES_PER_GROUP;
        final byte[] values = scratch.unpack(buffer, offset + (index >> 2), numGroups, 2, SPREAD_2, SHIFTS_2, (byte) 3);
        widenDelta(values, numValues, out, outPosition + i, base);
        i += numValues;
        index += numValues;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      if (table.length > 4) {
        super.getTable(out, outPosition, startIndex, length, table);
        return;
      }
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x3) != 0 && i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
      final int numGroups = (length - i) / VALUES_PER_GROUP;
      if (numGroups > 0) {
        final int numValues = numGroups * VALUES_PER_GROUP;
        final byte[] values = scratch.unpack(buffer, offset + (index >> 2), numGroups, 2, SPREAD_2, SHIFTS_2, (byte) 3);
        widenTable2(values, numValues, out, outPosition + i, table);
        i += numValues;
        index += numValues;
      }
      while (i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
    }
  }

  /**
   * Only {@code getDelta} is vectorized: a 16 entry table does not fit in a single long vector on common hardware, and
   * the JDK 21 Vector API has no two vector {@code selectFrom}.
   */
  static final class SimdSize4Des extends VSizeLongSerde.Size4Des
  {
    private final Scratch scratch = new Scratch();

    SimdSize4Des(ByteBuffer buffer, int bufferOffset)
    {
      super(buffer, bufferOffset);
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x1) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++);
      }
      final int numGroups = (length - i) / VALUES_PER_GROUP;
      if (numGroups > 0) {
        final int numValues = numGroups * VALUES_PER_GROUP;
        final byte[] values =
            scratch.unpack(buffer, offset + (index >> 1), numGroups, 4, SPREAD_4, SHIFTS_4, (byte) 0xF);
        widenDelta(values, numValues, out, outPosition + i, base);
        i += numValues;
        index += numValues;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }
  }

  /**
   * Only {@code getDelta} is vectorized, the table lookup of 8 bit values would need a gather.
   */
  static final class SimdSize8Des extends VSizeLongSerde.Size8Des
  {
    private final Scratch scratch = new Scratch();

    SimdSize8Des(ByteBuffer buffer, int bufferOffset)
    {
      super(buffer, bufferOffset);
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      final int numValues = length - length % VALUES_PER_GROUP;
      if (numValues > 0) {
        widenDelta(scratch.copy(buffer, offset + startIndex, numValues), numValues, out, outPosition, base);
      }
      for (int i = numValues; i < length; i++) {
        out[outPosition + i] = base + (buffer.get(offset + startIndex + i) & 0xFF);
      }
    }
  }

  static final class SimdSize16Des extends VSizeLongSerde.Size16Des
  {
    private final Scratch scratch = new Scratch();

    SimdSize16Des(ByteBuffer buffer, int bufferOffset)
    {
      super(buffer, bufferOffset);
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      final int numValues = length - length % VALUES_PER_GROUP;
      if (numValues > 0) {
        final byte[] values = scratch.copy(buffer, offset + (startIndex << 1), numValues * Short.BYTES);
        for (int i = 0; i < numValues; i += LONG_LANES) {
          widen(values, i * Short.BYTES, WIDEN_SHUFFLE_16, WIDEN_MASK_16).add(base).intoArray(out, outPosition + i);
        }
      }
      for (int i = numValues; i < length; i++) {
        out[outPosition + i] = base + get(startIndex + i);
      }
    }
  }
}
