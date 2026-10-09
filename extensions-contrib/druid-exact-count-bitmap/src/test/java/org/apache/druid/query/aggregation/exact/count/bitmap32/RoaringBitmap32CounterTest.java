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

package org.apache.druid.query.aggregation.exact.count.bitmap32;

import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class RoaringBitmap32CounterTest
{
  private static Set<Integer> randomValues(final Random random, final int count)
  {
    final Set<Integer> values = new HashSet<>();
    for (int i = 0; i < count; i++) {
      switch (random.nextInt(3)) {
        case 0:
          values.add(random.nextInt(1 << 20)); // dense
          break;
        case 1:
          values.add(random.nextInt()); // anywhere, including negative ints
          break;
        default:
          values.add(-1 - random.nextInt(1000)); // the end of the unsigned range
          break;
      }
    }
    return values;
  }

  private static RoaringBitmap32Counter counterOf(final Set<Integer> values)
  {
    final RoaringBitmap32Counter counter = new RoaringBitmap32Counter();
    for (int value : values) {
      counter.add(value);
    }
    return counter;
  }

  private static byte[] bytes(final RoaringBitmap32Counter counter)
  {
    return counter.toByteBuffer().array();
  }

  private static Set<Integer> contents(final byte[] bytes)
  {
    final RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes)));
    }
    catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
    final Set<Integer> values = new HashSet<>();
    bitmap.forEach((org.roaringbitmap.IntConsumer) values::add);
    Assertions.assertEquals(values.size(), bitmap.getLongCardinality());
    return values;
  }

  @Test
  void testSerializedFormIsAPortableRoaringBitmap()
  {
    final Random random = new Random(1);
    for (int count : new int[]{0, 1, 10, 5000}) {
      final Set<Integer> values = randomValues(random, count);
      final byte[] written = bytes(counterOf(values));
      Assertions.assertEquals(values, contents(written));

      // and the same bytes as a plain roaring bitmap of the same values
      final MutableRoaringBitmap expected = new MutableRoaringBitmap();
      values.forEach(expected::add);
      expected.runOptimize();
      final ByteBuffer expectedBuffer = ByteBuffer.allocate(expected.serializedSizeInBytes())
                                                  .order(ByteOrder.LITTLE_ENDIAN);
      expected.serialize(expectedBuffer);
      Assertions.assertArrayEquals(expectedBuffer.array(), written);
    }
  }

  @Test
  void testRoundTrip()
  {
    final Set<Integer> values = randomValues(new Random(2), 20000);
    final RoaringBitmap32Counter copy = RoaringBitmap32Counter.fromBytes(bytes(counterOf(values)));
    Assertions.assertEquals(values.size(), copy.getCardinality());
    Assertions.assertEquals(values, contents(bytes(copy)));
  }

  @Test
  void testFoldIsSetUnion()
  {
    final Random random = new Random(3);
    final RoaringBitmap32Counter union = new RoaringBitmap32Counter();
    final Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      final Set<Integer> values = randomValues(random, 1 + random.nextInt(300));
      expected.addAll(values);
      // alternate between values that are views over serialized data and values that were built in memory
      union.fold(i % 2 == 0 ? RoaringBitmap32Counter.fromBytes(bytes(counterOf(values))) : counterOf(values));
      if (i % 7 == 0) {
        // reading in between must not leave a stale result behind
        Assertions.assertEquals(expected.size(), union.getCardinality());
        Assertions.assertEquals(expected, contents(bytes(union)));
      }
    }
    Assertions.assertEquals(expected.size(), union.getCardinality());
    Assertions.assertEquals(expected, contents(bytes(union)));
  }

  @Test
  void testFoldIntoViewAndFoldOfSelfAndNull()
  {
    final Set<Integer> first = randomValues(new Random(4), 100);
    final Set<Integer> second = randomValues(new Random(5), 100);
    final RoaringBitmap32Counter view = RoaringBitmap32Counter.fromBytes(bytes(counterOf(first)));
    view.fold(counterOf(second));
    view.fold(null);
    view.fold(view);
    final Set<Integer> expected = new HashSet<>(first);
    expected.addAll(second);
    Assertions.assertEquals(expected.size(), view.getCardinality());
    Assertions.assertEquals(expected, contents(bytes(view)));
  }

  @Test
  void testFoldedCounterCanStillBeModified()
  {
    final RoaringBitmap32Counter source = new RoaringBitmap32Counter();
    source.add(1);
    final RoaringBitmap32Counter union = new RoaringBitmap32Counter();
    union.fold(source);
    Assertions.assertEquals(1, union.getCardinality());

    // the folded counter is a build counter that is still being written to
    source.add(2);
    source.add(1 << 20);
    Assertions.assertEquals(1, union.getCardinality());
    Assertions.assertEquals(3, source.getCardinality());
  }

  @Test
  void testAddAfterReadingAndFolding()
  {
    final RoaringBitmap32Counter counter = new RoaringBitmap32Counter();
    counter.add(1);
    counter.add(1 << 20);
    Assertions.assertEquals(2, counter.getCardinality());
    counter.fold(counterOf(new HashSet<>(Arrays.asList(2, 3, 1 << 20))));
    counter.add(4);
    Assertions.assertEquals(5, counter.getCardinality());
    Assertions.assertEquals(5, contents(bytes(counter)).size());
  }

  @Test
  void testZeroIsAValue()
  {
    final RoaringBitmap32Counter counter = new RoaringBitmap32Counter();
    counter.add(0);
    counter.add(0);
    counter.add(7);
    Assertions.assertEquals(2, counter.getCardinality());
  }

  @Test
  void testReadsFromRegionOfLargerBufferWithoutChangingIt()
  {
    final Set<Integer> values = randomValues(new Random(6), 500);
    final byte[] serialized = bytes(counterOf(values));
    final ByteBuffer larger = ByteBuffer.allocateDirect(serialized.length + 20).order(ByteOrder.BIG_ENDIAN);
    larger.position(10);
    larger.put(serialized);
    larger.position(10);
    larger.limit(10 + serialized.length);

    final RoaringBitmap32Counter counter = RoaringBitmap32Counter.fromByteBuffer(larger);
    Assertions.assertEquals(10, larger.position());
    Assertions.assertEquals(10 + serialized.length, larger.limit());
    Assertions.assertEquals(ByteOrder.BIG_ENDIAN, larger.order());
    Assertions.assertEquals(values.size(), counter.getCardinality());
    Assertions.assertEquals(values, contents(bytes(counter)));
  }

  @Test
  void testObjectStrategyReadsWithoutCopyAndWritesTheSameBytes()
  {
    final Bitmap32ExactCountObjectStrategy strategy = new Bitmap32ExactCountObjectStrategy();
    final Set<Integer> values = randomValues(new Random(7), 500);
    final byte[] serialized = strategy.toBytes(counterOf(values));
    final ByteBuffer column = ByteBuffer.allocate(serialized.length + 3);
    column.put(new byte[]{9, 9, 9});
    column.put(serialized);
    column.position(3);

    final Bitmap32 read = strategy.fromByteBuffer(column, serialized.length);
    Assertions.assertEquals(values.size(), read.getCardinality());
    Assertions.assertArrayEquals(serialized, strategy.toBytes(read));
  }

  @Test
  void testObjectStrategyStoresNullAndEmptyAsNoBytes()
  {
    final Bitmap32ExactCountObjectStrategy strategy = new Bitmap32ExactCountObjectStrategy();
    Assertions.assertEquals(0, strategy.toBytes(null).length);
    Assertions.assertEquals(0, strategy.toBytes(new RoaringBitmap32Counter()).length);

    final Bitmap32 read = strategy.fromByteBuffer(ByteBuffer.allocate(0), 0);
    Assertions.assertNotNull(read);
    Assertions.assertEquals(0, read.getCardinality());
  }

  @Test
  void testCorruptDataThrows()
  {
    final byte[] valid = bytes(counterOf(randomValues(new Random(8), 2000)));
    Assertions.assertThrows(RuntimeException.class, () -> RoaringBitmap32Counter.fromBytes(new byte[0]));
    Assertions.assertThrows(RuntimeException.class, () -> RoaringBitmap32Counter.fromBytes(new byte[]{1, 2, 3, 4, 5}));
    Assertions.assertThrows(
        RuntimeException.class,
        () -> RoaringBitmap32Counter.fromBytes(Arrays.copyOf(valid, 6)).getCardinality()
    );
  }

  @Test
  void testToIntChecksTheRange()
  {
    Assertions.assertEquals(0, RoaringBitmap32Counter.toInt(0L));
    Assertions.assertEquals(Integer.MAX_VALUE, RoaringBitmap32Counter.toInt(Integer.MAX_VALUE));
    Assertions.assertEquals(Integer.MIN_VALUE, RoaringBitmap32Counter.toInt(Integer.MIN_VALUE));
    Assertions.assertThrows(IAE.class, () -> RoaringBitmap32Counter.toInt(Integer.MAX_VALUE + 1L));
    Assertions.assertThrows(IAE.class, () -> RoaringBitmap32Counter.toInt(Integer.MIN_VALUE - 1L));
    Assertions.assertThrows(IAE.class, () -> RoaringBitmap32Counter.toInt(1L << 40));
  }

  @Test
  void testFromBytesDoesNotKeepTheCallersArray()
  {
    final Set<Integer> values = randomValues(new Random(9), 300);
    final byte[] serialized = bytes(counterOf(values));
    final RoaringBitmap32Counter counter = RoaringBitmap32Counter.fromBytes(serialized);
    final RoaringBitmap32Counter union = new RoaringBitmap32Counter();
    union.fold(counter);

    // the caller reuses its array
    Arrays.fill(serialized, (byte) 0);

    Assertions.assertEquals(values.size(), counter.getCardinality());
    Assertions.assertEquals(values.size(), union.getCardinality());
    Assertions.assertEquals(values, contents(bytes(union)));
  }

  @Test
  void testFoldWhileTheOtherCounterIsBeingWrittenTo() throws Exception
  {
    final RoaringBitmap32Counter source = new RoaringBitmap32Counter();
    final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean(false);
    final java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    final java.util.concurrent.Future<?> writer = executor.submit(() -> {
      for (int i = 1; i <= 200_000; i++) {
        source.add(i * 3);
      }
      done.set(true);
    });

    try {
      long previous = 0;
      while (!done.get()) {
        // every snapshot is complete and consistent: it can be read, and only grows
        final RoaringBitmap32Counter accumulator = new RoaringBitmap32Counter();
        accumulator.fold(source);
        final long cardinality = accumulator.getCardinality();
        Assertions.assertTrue(cardinality >= previous);
        Assertions.assertEquals(cardinality, contents(bytes(accumulator)).size());
        previous = cardinality;
      }
      writer.get();
    }
    finally {
      executor.shutdownNow();
    }
    Assertions.assertEquals(200_000, source.getCardinality());
  }
}
