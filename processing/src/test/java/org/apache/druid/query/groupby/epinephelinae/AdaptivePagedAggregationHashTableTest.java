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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AdaptivePagedAggregationHashTableTest extends InitializedNullHandlingTest
{
  @Test
  public void testSmallTableStaysInline()
  {
    final TestTable testTable = makeTable(8);
    final AdaptivePagedAggregationHashTable<IntKey> table = testTable.table;
    table.init();
    testTable.selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 2L)));

    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(table.aggregate(new IntKey(i)).isOk());
    }

    Assertions.assertFalse(table.isPaged());
    assertEntries(table, 2, 2L, 1L);
    table.close();
    testTable.lease.close();
    Assertions.assertEquals(0, testTable.pool.getUsedResourcesCount());
  }

  @Test
  public void testPromotionPreservesRawAggregationStateAndContinuesUpdating()
  {
    final TestTable testTable = makeTable(16);
    final AdaptivePagedAggregationHashTable<IntKey> table = testTable.table;
    table.init();
    testTable.selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 2L)));

    int key = 0;
    while (!table.isPaged()) {
      Assertions.assertTrue(table.aggregate(new IntKey(key++)).isOk());
    }
    testTable.selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 3L)));
    for (int i = 0; i < key; i++) {
      Assertions.assertTrue(table.aggregate(new IntKey(i)).isOk());
    }

    assertEntries(table, key, 5L, 2L);
    table.close();
    testTable.lease.close();
    Assertions.assertEquals(0, testTable.pool.getUsedResourcesCount());
  }

  @Test
  public void testFailedPromotionLeavesInlineTableAvailableForSpill()
  {
    final TestTable testTable = makeTable(2);
    final AdaptivePagedAggregationHashTable<IntKey> table = testTable.table;
    table.init();
    testTable.selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));

    int successfulKeys = 0;
    AggregateResult result;
    do {
      result = table.aggregate(new IntKey(successfulKeys));
      if (result.isOk()) {
        successfulKeys++;
      }
    } while (result.isOk());

    Assertions.assertFalse(table.isPaged());
    Assertions.assertEquals(successfulKeys, table.getSize());
    assertEntries(table, successfulKeys, 1L, 1L);
    table.close();
    testTable.lease.close();
    Assertions.assertEquals(0, testTable.pool.getUsedResourcesCount());
  }

  @Test
  public void testResetKeepsPagedModeWithoutRequiringPromotionAgain()
  {
    final TestTable testTable = makeTable(16);
    final AdaptivePagedAggregationHashTable<IntKey> table = testTable.table;
    table.init();
    testTable.selectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));
    for (int key = 0; !table.isPaged(); key++) {
      Assertions.assertTrue(table.aggregate(new IntKey(key)).isOk());
    }

    table.reset();

    Assertions.assertTrue(table.isPaged());
    Assertions.assertEquals(0, table.getSize());
    Assertions.assertTrue(table.aggregate(new IntKey(0)).isOk());
    assertEntries(table, 1, 1L, 1L);
    table.close();
    testTable.lease.close();
    Assertions.assertEquals(0, testTable.pool.getUsedResourcesCount());
  }

  private static void assertEntries(
      final AdaptivePagedAggregationHashTable<IntKey> table,
      final int count,
      final long sum,
      final long rowCount
  )
  {
    final List<Grouper.Entry<IntKey>> expected = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      expected.add(new ReusableEntry<>(new IntKey(i), new Object[]{sum, rowCount}));
    }
    GrouperTestUtil.assertEntriesEquals(expected.iterator(), table.iterator(true));
  }

  private static TestTable makeTable(final int maximumPages)
  {
    final int pageSize = 256;
    final GroupByTestColumnSelectorFactory selectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final DefaultBlockingPool<ByteBuffer> pool = new DefaultBlockingPool<>(() -> ByteBuffer.allocateDirect(4_096), 1);
    final MergeMemoryManager manager = new MergeMemoryManager(
        new BlockingPoolMergeMemoryBackingAllocator(pool, 4_096),
        pageSize
    );
    final MergeMemoryLease lease = manager.acquireMinimum(
        new QueryResourceId("query"),
        2,
        maximumPages,
        0
    );
    final AdaptivePagedAggregationHashTable<IntKey> table = new AdaptivePagedAggregationHashTable<>(
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
    return new TestTable(table, selectorFactory, pool, lease);
  }

  private static class TestTable
  {
    private final AdaptivePagedAggregationHashTable<IntKey> table;
    private final GroupByTestColumnSelectorFactory selectorFactory;
    private final DefaultBlockingPool<ByteBuffer> pool;
    private final MergeMemoryLease lease;

    private TestTable(
        final AdaptivePagedAggregationHashTable<IntKey> table,
        final GroupByTestColumnSelectorFactory selectorFactory,
        final DefaultBlockingPool<ByteBuffer> pool,
        final MergeMemoryLease lease
    )
    {
      this.table = table;
      this.selectorFactory = selectorFactory;
      this.pool = pool;
      this.lease = lease;
    }
  }
}
