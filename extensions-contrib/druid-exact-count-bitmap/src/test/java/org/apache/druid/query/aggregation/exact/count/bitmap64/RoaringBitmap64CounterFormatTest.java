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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Checks that {@link RoaringBitmap64Counter} reads and writes the serialized form of {@link Roaring64NavigableMap}
 * that segments written by earlier versions contain, and that merging behaves like a set union.
 */
class RoaringBitmap64CounterFormatTest
{
  private static Set<Long> randomValues(final Random random, final int count)
  {
    final Set<Long> values = new HashSet<>();
    for (int i = 0; i < count; i++) {
      switch (random.nextInt(4)) {
        case 0:
          values.add((long) random.nextInt(1 << 20)); // dense, high part 0
          break;
        case 1:
          values.add(((long) random.nextInt(8) << 32) + random.nextInt(1 << 16)); // a few high parts
          break;
        case 2:
          values.add(random.nextLong()); // anywhere, including negative longs
          break;
        default:
          values.add(-1L - random.nextInt(1000)); // the highest high part, which is negative as a signed int
          break;
      }
    }
    return values;
  }

  private static RoaringBitmap64Counter counterOf(final Set<Long> values)
  {
    final RoaringBitmap64Counter counter = new RoaringBitmap64Counter();
    for (long value : values) {
      counter.add(value);
    }
    return counter;
  }

  private static byte[] legacyBytes(final Set<Long> values, final boolean signedLongs) throws IOException
  {
    final Roaring64NavigableMap map = new Roaring64NavigableMap(signedLongs, false);
    for (long value : values) {
      map.addLong(value);
    }
    map.runOptimize();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    map.serialize(new DataOutputStream(out));
    return out.toByteArray();
  }

  private static Set<Long> legacyContents(final byte[] bytes) throws IOException
  {
    final Roaring64NavigableMap map = new Roaring64NavigableMap();
    map.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    final Set<Long> values = new HashSet<>();
    final LongIterator iterator = map.getLongIterator();
    while (iterator.hasNext()) {
      values.add(iterator.next());
    }
    Assertions.assertEquals(values.size(), map.getLongCardinality());
    return values;
  }

  private static byte[] bytes(final RoaringBitmap64Counter counter)
  {
    return counter.toByteBuffer().array();
  }

  @Test
  void testReadsBytesWrittenByRoaring64NavigableMap() throws IOException
  {
    final Random random = new Random(1);
    for (boolean signedLongs : new boolean[]{false, true}) {
      for (int count : new int[]{0, 1, 10, 5000}) {
        final Set<Long> values = randomValues(random, count);
        final RoaringBitmap64Counter counter = RoaringBitmap64Counter.fromBytes(legacyBytes(values, signedLongs));
        Assertions.assertEquals(values.size(), counter.getCardinality());
        Assertions.assertEquals(values, legacyContents(bytes(counter)));
      }
    }
  }

  @Test
  void testWritesBytesReadableByRoaring64NavigableMap() throws IOException
  {
    final Random random = new Random(2);
    for (int count : new int[]{0, 1, 10, 5000}) {
      final Set<Long> values = randomValues(random, count);
      final byte[] written = bytes(counterOf(values));
      Assertions.assertEquals(values, legacyContents(written));
      // and identical to what the previous implementation wrote for the same values
      Assertions.assertArrayEquals(legacyBytes(values, true), written);
    }
  }

  @Test
  void testRoundTrip() throws IOException
  {
    final Set<Long> values = randomValues(new Random(3), 20000);
    final RoaringBitmap64Counter copy = RoaringBitmap64Counter.fromBytes(bytes(counterOf(values)));
    Assertions.assertEquals(values.size(), copy.getCardinality());
    Assertions.assertEquals(values, legacyContents(bytes(copy)));
  }

  @Test
  void testFoldIsSetUnion() throws IOException
  {
    final Random random = new Random(4);
    final RoaringBitmap64Counter union = new RoaringBitmap64Counter();
    final Set<Long> expected = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      final Set<Long> values = randomValues(random, 1 + random.nextInt(300));
      expected.addAll(values);
      // alternate between values that are views over serialized data and values that were built in memory
      union.fold(i % 2 == 0 ? RoaringBitmap64Counter.fromBytes(bytes(counterOf(values))) : counterOf(values));
      if (i % 7 == 0) {
        // reading in between must not leave a stale result behind
        Assertions.assertEquals(expected.size(), union.getCardinality());
        Assertions.assertEquals(expected, legacyContents(bytes(union)));
      }
    }
    Assertions.assertEquals(expected.size(), union.getCardinality());
    Assertions.assertEquals(expected, legacyContents(bytes(union)));
  }

  @Test
  void testFoldIntoViewAndFoldOfSelfAndNull() throws IOException
  {
    final Set<Long> first = randomValues(new Random(5), 100);
    final Set<Long> second = randomValues(new Random(6), 100);
    final RoaringBitmap64Counter view = RoaringBitmap64Counter.fromBytes(bytes(counterOf(first)));
    view.fold(counterOf(second));
    view.fold(null);
    view.fold(view);
    final Set<Long> expected = new HashSet<>(first);
    expected.addAll(second);
    Assertions.assertEquals(expected.size(), view.getCardinality());
    Assertions.assertEquals(expected, legacyContents(bytes(view)));
  }

  @Test
  void testFoldedCounterCanStillBeModified() throws IOException
  {
    final RoaringBitmap64Counter source = new RoaringBitmap64Counter();
    source.add(1);
    final RoaringBitmap64Counter union = new RoaringBitmap64Counter();
    union.fold(source);
    Assertions.assertEquals(1, union.getCardinality());

    // the folded counter is a build counter that is still being written to
    source.add(2);
    source.add(1L << 40);
    Assertions.assertEquals(1, union.getCardinality());
    Assertions.assertEquals(3, source.getCardinality());
  }

  @Test
  void testAddAfterReadingAndFolding() throws IOException
  {
    final RoaringBitmap64Counter counter = new RoaringBitmap64Counter();
    counter.add(1);
    counter.add(1L << 33);
    Assertions.assertEquals(2, counter.getCardinality());
    counter.fold(counterOf(new HashSet<>(Arrays.asList(2L, 3L, 1L << 33))));
    counter.add(4);
    Assertions.assertEquals(5, counter.getCardinality());
    Assertions.assertEquals(5, legacyContents(bytes(counter)).size());
  }

  @Test
  void testReadsFromRegionOfLargerBufferWithoutChangingIt() throws IOException
  {
    final Set<Long> values = randomValues(new Random(7), 500);
    final byte[] serialized = bytes(counterOf(values));
    final ByteBuffer larger = ByteBuffer.allocateDirect(serialized.length + 20).order(ByteOrder.LITTLE_ENDIAN);
    larger.position(10);
    larger.put(serialized);
    larger.position(10);
    larger.limit(10 + serialized.length);

    final RoaringBitmap64Counter counter = RoaringBitmap64Counter.fromByteBuffer(larger);
    Assertions.assertEquals(10, larger.position());
    Assertions.assertEquals(10 + serialized.length, larger.limit());
    Assertions.assertEquals(ByteOrder.LITTLE_ENDIAN, larger.order());
    Assertions.assertEquals(values.size(), counter.getCardinality());
    Assertions.assertEquals(values, legacyContents(bytes(counter)));
  }

  @Test
  void testObjectStrategyReadsWithoutCopyAndWritesTheSameBytes() throws IOException
  {
    final Bitmap64ExactCountObjectStrategy strategy = new Bitmap64ExactCountObjectStrategy();
    final Set<Long> values = randomValues(new Random(8), 500);
    final byte[] serialized = strategy.toBytes(counterOf(values));
    final ByteBuffer column = ByteBuffer.allocate(serialized.length + 3);
    column.put(new byte[]{9, 9, 9});
    column.put(serialized);
    column.position(3);

    final Bitmap64 read = strategy.fromByteBuffer(column, serialized.length);
    Assertions.assertEquals(values.size(), read.getCardinality());
    Assertions.assertArrayEquals(serialized, strategy.toBytes(read));
  }

  @Test
  void testCorruptDataThrows()
  {
    final byte[] valid = bytes(counterOf(randomValues(new Random(9), 200)));
    Assertions.assertThrows(RuntimeException.class, () -> RoaringBitmap64Counter.fromBytes(new byte[0]));
    Assertions.assertThrows(RuntimeException.class, () -> RoaringBitmap64Counter.fromBytes(new byte[]{0, 0, 0}));
    Assertions.assertThrows(
        RuntimeException.class,
        () -> RoaringBitmap64Counter.fromBytes(Arrays.copyOf(valid, valid.length / 2))
    );
    // a huge number of high parts
    Assertions.assertThrows(
        RuntimeException.class,
        () -> RoaringBitmap64Counter.fromBytes(new byte[]{0, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1, 2, 3})
    );
  }

  @Test
  void testEmpty() throws IOException
  {
    final RoaringBitmap64Counter empty = new RoaringBitmap64Counter();
    Assertions.assertEquals(0, empty.getCardinality());
    Assertions.assertEquals(0, RoaringBitmap64Counter.fromBytes(bytes(empty)).getCardinality());
    Assertions.assertArrayEquals(legacyBytes(new HashSet<>(), true), bytes(empty));
  }
}
