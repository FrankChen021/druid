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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.collections.CombiningIterator;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent paged grouper using exclusive single-writer lanes. It deliberately has no hash-routed shared-write mode.
 */
public class PagedConcurrentGrouper<KeyType> implements Grouper<KeyType>
{
  private final List<SpillingGrouper<KeyType>> lanes;
  private final List<MergeMemoryLease> laneLeases;
  private final ThreadLocal<SpillingGrouper<KeyType>> threadLane;
  private final KeySerdeFactory<KeyType> keySerdeFactory;
  private final AggregatorFactory[] aggregatorFactories;
  private final java.util.Comparator<Entry<KeyType>> keyComparator;
  private boolean initialized;
  private boolean closed;

  public PagedConcurrentGrouper(
      final GroupByQueryConfig config,
      final MergeMemoryLease memoryLease,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int laneCount,
      final boolean sortHasNonGroupingFields,
      final GroupByStatsProvider.PerQueryStats perQueryStats
  )
  {
    this.keySerdeFactory = keySerdeFactory;
    this.aggregatorFactories = aggregatorFactories;
    this.keyComparator = keySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.lanes = new ArrayList<>(laneCount);
    this.laneLeases = new ArrayList<>(laneCount);
    final int guaranteedPagesPerLane = PagedAggregationHashTable.minimumPageCount(
        memoryLease.pageSize(),
        config.getBufferGrouperInitialBuckets()
    );
    try {
      for (int i = 0; i < laneCount; i++) {
        final MergeMemoryLease laneLease = new LaneMergeMemoryLease(memoryLease, guaranteedPagesPerLane);
        laneLeases.add(laneLease);
        lanes.add(
            new SpillingGrouper<>(
                laneLease,
                keySerdeFactory,
                columnSelectorFactory,
                aggregatorFactories,
                config.getBufferGrouperMaxSize(),
                config.getBufferGrouperMaxLoadFactor(),
                config.getBufferGrouperInitialBuckets(),
                temporaryStorage,
                spillMapper,
                true,
                sortHasNonGroupingFields,
                config.getMinSpillFileSize(),
                perQueryStats
            )
        );
      }
    }
    catch (Throwable t) {
      lanes.forEach(SpillingGrouper::close);
      laneLeases.forEach(MergeMemoryLease::close);
      throw t;
    }
    final AtomicInteger nextLane = new AtomicInteger();
    this.threadLane = ThreadLocal.withInitial(() -> {
      final int laneNumber = nextLane.getAndIncrement();
      if (laneNumber >= lanes.size()) {
        throw new ISE("More aggregation threads than admitted lanes[%d]", lanes.size());
      }
      return lanes.get(laneNumber);
    });
  }

  @Override
  public void init()
  {
    if (!initialized) {
      lanes.forEach(SpillingGrouper::init);
      initialized = true;
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(final KeyType key, final int keyHash)
  {
    checkUsable();
    return threadLane.get().aggregate(key, keyHash);
  }

  @Override
  public void reset()
  {
    checkUsable();
    lanes.forEach(SpillingGrouper::reset);
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    checkUsable();
    final List<CloseableIterator<Entry<KeyType>>> iterators = new ArrayList<>(lanes.size());
    lanes.forEach(lane -> iterators.add(lane.iterator(sorted)));
    if (!sorted) {
      return CloseableIterators.concat(iterators);
    }
    final CloseableIterator<Entry<KeyType>> merged = CloseableIterators.mergeSorted(iterators, keyComparator);
    final ReusableEntry<KeyType> reusableEntry = ReusableEntry.create(
        keySerdeFactory.factorize(),
        aggregatorFactories.length
    );
    return CloseableIterators.wrap(
        new CombiningIterator<>(
            merged,
            keyComparator,
            (entry1, entry2) -> {
              if (entry2 == null) {
                reusableEntry.setKey(keySerdeFactory.copyKey(entry1.getKey()));
                System.arraycopy(entry1.getValues(), 0, reusableEntry.getValues(), 0, entry1.getValues().length);
              } else {
                for (int i = 0; i < aggregatorFactories.length; i++) {
                  reusableEntry.getValues()[i] = aggregatorFactories[i].combine(
                      reusableEntry.getValues()[i],
                      entry2.getValues()[i]
                  );
                }
              }
              return reusableEntry;
            }
        ),
        merged
    );
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    lanes.forEach(SpillingGrouper::close);
    laneLeases.forEach(MergeMemoryLease::close);
  }

  private void checkUsable()
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }
    if (closed) {
      throw new ISE("Grouper is closed");
    }
  }
}
