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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Single-writer grouper that keeps small tables in one compact {@link BufferHashGrouper} page and promotes to a
 * {@link PagedAggregationHashTable} only when that page fills.
 */
public final class AdaptivePagedAggregationHashTable<KeyType> extends BufferHashGrouper<KeyType>
{
  private final MergeMemoryLease memoryLease;
  private final MergeMemoryPage inlinePage;
  private final int configuredMaxSize;
  private final float configuredMaxLoadFactor;
  private final int configuredInitialBuckets;

  private PagedAggregationHashTable<KeyType> pagedTable;
  private long maxObservedUsedBytes;
  private boolean reachedCapacity;

  public AdaptivePagedAggregationHashTable(
      final MergeMemoryLease memoryLease,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int configuredMaxSize,
      final float maxLoadFactor,
      final int initialBuckets
  )
  {
    this(
        memoryLease,
        acquireInlinePage(memoryLease),
        keySerde,
        aggregators,
        configuredMaxSize,
        maxLoadFactor,
        initialBuckets
    );
  }

  private AdaptivePagedAggregationHashTable(
      final MergeMemoryLease memoryLease,
      final MergeMemoryPage inlinePage,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int configuredMaxSize,
      final float maxLoadFactor,
      final int initialBuckets
  )
  {
    super(
        inlinePage::get,
        keySerde,
        aggregators,
        configuredMaxSize,
        maxLoadFactor,
        initialBuckets,
        true
    );
    this.memoryLease = memoryLease;
    this.inlinePage = inlinePage;
    this.configuredMaxSize = configuredMaxSize;
    this.configuredMaxLoadFactor = maxLoadFactor;
    this.configuredInitialBuckets = initialBuckets;
    this.maxObservedUsedBytes = memoryLease.pageSize();
  }

  @Override
  public AggregateResult aggregate(final KeyType key, final int keyHash)
  {
    if (pagedTable != null) {
      final AggregateResult result = pagedTable.aggregate(key, keyHash);
      if (!result.isOk()) {
        reachedCapacity = true;
      }
      return result;
    }

    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (keyBuffer == null) {
      return Groupers.dictionaryFull(0);
    }
    if (keyBuffer.remaining() != keySize) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          keyBuffer.remaining(),
          keySize
      );
    }

    final int bucket = hashTable.findBucketWithAutoGrowth(keyBuffer, keyHash, () -> {});
    if (bucket < 0) {
      if (promote()) {
        final AggregateResult result = pagedTable.aggregate(key, keyHash);
        if (!result.isOk()) {
          reachedCapacity = true;
        }
        return result;
      }
      reachedCapacity = true;
      return Groupers.hashTableFull(0);
    }

    final int bucketStartOffset = hashTable.getOffsetForBucket(bucket);
    final boolean bucketWasUsed = hashTable.isBucketUsed(bucket);
    final ByteBuffer tableBuffer = hashTable.getTableBuffer();
    if (!bucketWasUsed) {
      hashTable.initializeNewBucketKey(bucket, keyBuffer, keyHash);
      aggregators.init(tableBuffer, bucketStartOffset + baseAggregatorOffset);
      newBucketHook(bucketStartOffset);
    }
    aggregators.aggregateBuffered(tableBuffer, bucketStartOffset + baseAggregatorOffset);
    return AggregateResult.ok();
  }

  @Override
  public void reset()
  {
    if (pagedTable == null) {
      super.reset();
    } else {
      pagedTable.reset();
    }
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    return pagedTable == null ? super.iterator(sorted) : pagedTable.iterator(sorted);
  }

  @Override
  public void close()
  {
    if (pagedTable == null) {
      super.close();
      inlinePage.close();
    } else {
      pagedTable.close();
    }
  }

  @Override
  public long getMaxMergeBufferUsedBytes()
  {
    return Math.max(maxObservedUsedBytes, pagedTable == null ? 0 : pagedTable.getMaxMergeBufferUsedBytes());
  }

  @Override
  public double getMaxSpillProximity()
  {
    if (reachedCapacity) {
      return 1.0;
    }
    return pagedTable == null ? super.getMaxSpillProximity() : pagedTable.getMaxSpillProximity();
  }

  @Override
  public int getSize()
  {
    return pagedTable == null ? super.getSize() : pagedTable.getSize();
  }

  @Override
  public int getGrowthCount()
  {
    return pagedTable == null ? super.getGrowthCount() : pagedTable.getGrowthCount();
  }

  boolean isPaged()
  {
    return pagedTable != null;
  }

  private boolean promote()
  {
    final int entryCount = super.getSize();
    final PagedAggregationHashTable<KeyType> candidate = new PagedAggregationHashTable<>(
        memoryLease,
        keySerde,
        aggregators,
        configuredMaxSize,
        configuredMaxLoadFactor,
        configuredInitialBuckets
    );
    candidate.init();
    if (!candidate.prepareForImport(entryCount)) {
      candidate.abortImport();
      return false;
    }

    maxObservedUsedBytes = Math.max(
        maxObservedUsedBytes,
        memoryLease.pageSize() + candidate.getMaxMergeBufferUsedBytes()
    );
    final ByteBuffer source = hashTable.getTableBuffer();
    int imported = 0;
    for (int bucket = 0; bucket < hashTable.getMaxBuckets(); bucket++) {
      if (hashTable.isBucketUsed(bucket)) {
        final int bucketOffset = hashTable.getOffsetForBucket(bucket);
        candidate.importEntry(
            source,
            bucketOffset + HASH_SIZE,
            bucketOffset + baseAggregatorOffset,
            source.getInt(bucketOffset) & Groupers.USED_FLAG_MASK
        );
        imported++;
      }
    }
    if (imported != entryCount) {
      throw new ISE("Imported [%,d] records from an inline table of size [%,d]", imported, entryCount);
    }

    inlinePage.close();
    pagedTable = candidate;
    return true;
  }

  private static MergeMemoryPage acquireInlinePage(final MergeMemoryLease memoryLease)
  {
    final List<MergeMemoryPage> pages = memoryLease.tryAcquirePages(1).orElseThrow(
        () -> new ISE("Unable to acquire initial inline aggregation page")
    );
    return pages.get(0);
  }
}
