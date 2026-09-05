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

import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryResourceId;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class PagedConcurrentGrouperTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @Test
  public void testExclusiveLanesCombineRepeatedKeys() throws Exception
  {
    final int laneCount = 2;
    final DefaultBlockingPool<ByteBuffer> pool = new DefaultBlockingPool<>(() -> ByteBuffer.allocateDirect(4096), 8);
    final MergeMemoryManager manager = new MergeMemoryManager(
        new BlockingPoolMergeMemoryBackingAllocator(pool, 4096),
        256
    );
    final int minimumPages = laneCount * PagedAggregationHashTable.minimumPageCount(256, 0);
    final MergeMemoryLease lease = manager.acquireMinimum(new QueryResourceId("query"), minimumPages, 128, 0);
    final GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();
    final LimitedTemporaryStorage storage = new LimitedTemporaryStorage(
        temporaryFolder.newFolder(),
        1024 * 1024,
        100,
        stats
    );
    final PagedConcurrentGrouper<ConcurrentGrouperTest.LongKey> grouper = new PagedConcurrentGrouper<>(
        new GroupByQueryConfig(),
        lease,
        new ConcurrentGrouperTest.TestKeySerdeFactory(),
        GrouperTestUtil.newColumnSelectorFactory(),
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        storage,
        new DefaultObjectMapper(),
        laneCount,
        false,
        stats
    );
    grouper.init();
    final ExecutorService executor = Execs.multiThreaded(laneCount, "paged-grouper-test-%d");
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int lane = 0; lane < laneCount; lane++) {
        futures.add(executor.submit(() -> {
          for (long key = 0; key < 100; key++) {
            Assertions.assertTrue(grouper.aggregate(new ConcurrentGrouperTest.LongKey(key)).isOk());
          }
        }));
      }
      for (Future<?> future : futures) {
        future.get();
      }

      final List<Grouper.Entry<ConcurrentGrouperTest.LongKey>> expected = new ArrayList<>();
      for (long key = 0; key < 100; key++) {
        expected.add(new ReusableEntry<>(new ConcurrentGrouperTest.LongKey(key), new Object[]{2L}));
      }
      try (CloseableIterator<Grouper.Entry<ConcurrentGrouperTest.LongKey>> iterator = grouper.iterator(true)) {
        GrouperTestUtil.assertEntriesEquals(expected.iterator(), iterator);
      }
    }
    finally {
      executor.shutdownNow();
      grouper.close();
      storage.close();
      lease.close();
    }
    Assertions.assertEquals(0, pool.getUsedResourcesCount());
  }
}
