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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.ToIntFunction;

public class PagedAggregationHashTableTest extends InitializedNullHandlingTest
{
  @Test
  public void testIndexAndPayloadGrowAcrossBackingBuffers()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final DefaultBlockingPool<ByteBuffer> pool = new DefaultBlockingPool<>(() -> ByteBuffer.allocateDirect(256), 8);
    final MergeMemoryManager manager = new MergeMemoryManager(
        new BlockingPoolMergeMemoryBackingAllocator(pool, 256),
        64
    );
    final MergeMemoryLease lease = manager.acquireMinimum(new QueryResourceId("query"), 2, 32, 0);
    final PagedAggregationHashTable<IntKey> table = new PagedAggregationHashTable<>(
        lease,
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            selectorFactory,
            ImmutableList.of(new LongSumAggregatorFactory("valueSum", "value"), new CountAggregatorFactory("count"))
        ),
        Integer.MAX_VALUE,
        0.7f,
        4
    );
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    for (int i = 0; i < 40; i++) {
      Assertions.assertTrue(table.aggregate(new IntKey(i)).isOk());
    }

    Assertions.assertTrue(manager.getBackingAllocationCount() > 1);
    Assertions.assertEquals(40, table.getSize());
    table.close();
    lease.close();
    Assertions.assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testGrowthKeepsRecordsStableAndValuesUpdatable()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 16_384, 256);
    table.init();

    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    for (int i = 0; i < 100; i++) {
      Assertions.assertTrue(table.aggregate(new IntKey(i)).isOk());
    }
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 11L)));
    for (int i = 0; i < 100; i++) {
      Assertions.assertTrue(table.aggregate(new IntKey(i)).isOk());
    }

    final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      expected.add(new ReusableEntry<>(new IntKey(i), new Object[]{21L, 2L}));
    }
    GrouperTestUtil.assertEntriesEquals(expected.iterator(), table.iterator(true));
    Assertions.assertTrue(table.getGrowthCount() > 0);
    Assertions.assertTrue(table.getPageCount() > 1);
  }

  @Test
  public void testResetReusesTable()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 4_096, 128);
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 3L)));
    Assertions.assertTrue(table.aggregate(new IntKey(1)).isOk());

    table.reset();
    Assertions.assertEquals(0, table.getSize());
    Assertions.assertEquals(0, table.getPageCount());
    Assertions.assertTrue(table.aggregate(new IntKey(2)).isOk());

    GrouperTestUtil.assertEntriesEquals(
        ImmutableList.<Grouper.Entry<IntKey>>of(new ReusableEntry<>(new IntKey(2), new Object[]{3L, 1L})).iterator(),
        table.iterator(true)
    );
  }

  @Test
  public void testMergeBufferUsageIncludesReservedIndexArena()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 4_096, 128);
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    Assertions.assertTrue(table.aggregate(new IntKey(1)).isOk());

    Assertions.assertTrue(
        table.getMaxMergeBufferUsedBytes() > 4L * Long.BYTES + 128,
        "usage must include the full reserved index arena, not only the four initial buckets"
    );
    Assertions.assertTrue(table.getMaxMergeBufferUsedBytes() <= 4_096);
  }

  @Test
  public void testReturnsFullWithoutLosingExistingKeyUpdates() throws IOException
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 1_024, 128);
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));

    int key = 0;
    while (table.aggregate(new IntKey(key)).isOk()) {
      key++;
    }
    Assertions.assertTrue(key > 0);
    Assertions.assertTrue(table.aggregate(new IntKey(0)).isOk());
    try (CloseableIterator<Grouper.Entry<IntKey>> iterator = table.iterator(false)) {
      while (iterator.hasNext()) {
        final Grouper.Entry<IntKey> entry = iterator.next();
        if (entry.getKey().intValue() == 0) {
          Assertions.assertArrayEquals(new Object[]{2L, 2L}, entry.getValues());
          return;
        }
      }
    }
    Assertions.fail("Expected existing key after the table became full");
  }

  @Test
  public void testCollisionsWrapAroundAndSurviveGrowth()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 4_096, 128);
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));

    final List<IntKey> collidingKeys = new ArrayList<>();
    for (int value = 0; collidingKeys.size() < 20; value++) {
      final IntKey key = new IntKey(value);
      if ((table.hashFunction().applyAsInt(key) & 3) == 3) {
        collidingKeys.add(key);
      }
    }
    for (final IntKey key : collidingKeys) {
      Assertions.assertTrue(table.aggregate(key).isOk());
    }

    final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
    for (final IntKey key : collidingKeys) {
      expected.add(new ReusableEntry<>(key, new Object[]{1L, 1L}));
    }
    expected.sort((left, right) -> Integer.compare(left.getKey().intValue(), right.getKey().intValue()));
    GrouperTestUtil.assertEntriesEquals(expected.iterator(), table.iterator(true));
    Assertions.assertTrue(table.getGrowthCount() > 0);
  }

  @Test
  public void testNonPowerOfTwoIndexPageUsesFallbackAddressing()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = makeTable(selectorFactory, 4_096, 24);
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 2L)));

    final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      final IntKey key = new IntKey(i);
      Assertions.assertTrue(table.aggregate(key).isOk());
      Assertions.assertTrue(table.aggregate(key).isOk());
      expected.add(new ReusableEntry<>(key, new Object[]{4L, 2L}));
    }

    GrouperTestUtil.assertEntriesEquals(expected.iterator(), table.iterator(true));
    Assertions.assertTrue(table.getGrowthCount() > 0);
  }

  @Test
  public void testEqualHashesRemainDistinctAcrossGrowth()
  {
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<IntKey> table = new PagedAggregationHashTable<>(
        Suppliers.ofInstance(ByteBuffer.allocate(4_096)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            selectorFactory,
            ImmutableList.of(new LongSumAggregatorFactory("valueSum", "value"), new CountAggregatorFactory("count"))
        ),
        Integer.MAX_VALUE,
        0.7f,
        4,
        128
    )
    {
      @Override
      public ToIntFunction<IntKey> hashFunction()
      {
        return ignored -> 7;
      }
    };
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 3L)));

    final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      final IntKey key = new IntKey(i);
      Assertions.assertTrue(table.aggregate(key).isOk());
      Assertions.assertTrue(table.aggregate(key).isOk());
      expected.add(new ReusableEntry<>(key, new Object[]{6L, 2L}));
    }

    GrouperTestUtil.assertEntriesEquals(expected.iterator(), table.iterator(true));
    Assertions.assertTrue(table.getGrowthCount() > 0);
  }

  @Test
  public void testWideEqualHashKeysRemainDistinct() throws IOException
  {
    final int keySize = 128;
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final PagedAggregationHashTable<byte[]> table = new PagedAggregationHashTable<>(
        Suppliers.ofInstance(ByteBuffer.allocate(4_096)),
        byteArrayKeySerde(keySize),
        AggregatorAdapters.factorizeBuffered(selectorFactory, ImmutableList.of(new CountAggregatorFactory("count"))),
        Integer.MAX_VALUE,
        0.7f,
        4,
        512
    );
    table.init();
    selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of()));
    final byte[] first = new byte[keySize];
    final byte[] second = new byte[keySize];
    second[keySize - 1] = 1;

    Assertions.assertTrue(table.aggregate(first, 7).isOk());
    Assertions.assertTrue(table.aggregate(first, 7).isOk());
    Assertions.assertTrue(table.aggregate(second, 7).isOk());
    Assertions.assertEquals(2, table.getSize());

    boolean sawFirst = false;
    boolean sawSecond = false;
    try (CloseableIterator<Grouper.Entry<byte[]>> iterator = table.iterator(false)) {
      while (iterator.hasNext()) {
        final Grouper.Entry<byte[]> entry = iterator.next();
        if (Arrays.equals(first, entry.getKey())) {
          Assertions.assertArrayEquals(new Object[]{2L}, entry.getValues());
          sawFirst = true;
        } else if (Arrays.equals(second, entry.getKey())) {
          Assertions.assertArrayEquals(new Object[]{1L}, entry.getValues());
          sawSecond = true;
        } else {
          Assertions.fail("Unexpected key");
        }
      }
    }
    Assertions.assertTrue(sawFirst);
    Assertions.assertTrue(sawSecond);
  }

  @Test
  public void testZeroWidthRecordUsesPhysicalStride() throws IOException
  {
    final Grouper.KeySerde<Object> emptyKeySerde = new Grouper.KeySerde<>()
    {
      @Override
      public int keySize()
      {
        return 0;
      }

      @Override
      public Class<Object> keyClazz()
      {
        return Object.class;
      }

      @Override
      public List<String> getDictionary()
      {
        return ImmutableList.of();
      }

      @Override
      public Long getDictionarySize()
      {
        return 0L;
      }

      @Override
      public ByteBuffer toByteBuffer(final Object key)
      {
        return ByteBuffer.allocate(0);
      }

      @Override
      public Object createKey()
      {
        return new Object();
      }

      @Override
      public void readFromByteBuffer(final Object key, final ByteBuffer buffer, final int position)
      {
        // Nothing to read.
      }

      @Override
      public Grouper.BufferComparator bufferComparator()
      {
        return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> 0;
      }

      @Override
      public Grouper.BufferComparator bufferComparatorWithAggregators(
          final AggregatorFactory[] aggregatorFactories,
          final int[] aggregatorOffsets
      )
      {
        return bufferComparator();
      }

      @Override
      public void reset()
      {
        // Nothing to reset.
      }
    };
    final PagedAggregationHashTable<Object> table = new PagedAggregationHashTable<>(
        Suppliers.ofInstance(ByteBuffer.allocate(128)),
        emptyKeySerde,
        AggregatorAdapters.factorizeBuffered(
            GrouperTestUtil.newColumnSelectorFactory(),
            ImmutableList.of()
        ),
        Integer.MAX_VALUE,
        0.7f,
        4,
        32
    );
    table.init();

    Assertions.assertTrue(table.aggregate(new Object(), 0).isOk());
    Assertions.assertTrue(table.aggregate(new Object(), 0).isOk());
    Assertions.assertEquals(1, table.getSize());
    try (CloseableIterator<Grouper.Entry<Object>> iterator = table.iterator(true)) {
      Assertions.assertTrue(iterator.hasNext());
      Assertions.assertEquals(0, iterator.next().getValues().length);
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  private static PagedAggregationHashTable<IntKey> makeTable(
      final GroupByTestColumnSelectorFactory selectorFactory,
      final int bufferSize,
      final int pageSize
  )
  {
    return new PagedAggregationHashTable<>(
        Suppliers.ofInstance(ByteBuffer.allocate(bufferSize)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            selectorFactory,
            ImmutableList.of(new LongSumAggregatorFactory("valueSum", "value"), new CountAggregatorFactory("count"))
        ),
        Integer.MAX_VALUE,
        0.7f,
        4,
        pageSize
    );
  }

  private static Grouper.KeySerde<byte[]> byteArrayKeySerde(final int keySize)
  {
    return new Grouper.KeySerde<>()
    {
      private final ByteBuffer buffer = ByteBuffer.allocate(keySize);

      @Override
      public int keySize()
      {
        return keySize;
      }

      @Override
      public Class<byte[]> keyClazz()
      {
        return byte[].class;
      }

      @Override
      public List<String> getDictionary()
      {
        return ImmutableList.of();
      }

      @Override
      public Long getDictionarySize()
      {
        return 0L;
      }

      @Override
      public ByteBuffer toByteBuffer(final byte[] key)
      {
        buffer.clear();
        buffer.put(key);
        buffer.flip();
        return buffer;
      }

      @Override
      public byte[] createKey()
      {
        return new byte[keySize];
      }

      @Override
      public void readFromByteBuffer(final byte[] key, final ByteBuffer source, final int position)
      {
        source.get(position, key);
      }

      @Override
      public Grouper.BufferComparator bufferComparator()
      {
        return (left, right, leftPosition, rightPosition) -> {
          for (int i = 0; i < keySize; i++) {
            final int comparison = Byte.compare(left.get(leftPosition + i), right.get(rightPosition + i));
            if (comparison != 0) {
              return comparison;
            }
          }
          return 0;
        };
      }

      @Override
      public Grouper.BufferComparator bufferComparatorWithAggregators(
          final AggregatorFactory[] aggregatorFactories,
          final int[] aggregatorOffsets
      )
      {
        return bufferComparator();
      }

      @Override
      public void reset()
      {
        // Nothing to reset.
      }
    };
  }
}
