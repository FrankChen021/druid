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

import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Single-writer aggregation hash table with a paged indirect index and stable records. */
public class PagedAggregationHashTable<KeyType> implements SpillableGrouper<KeyType>
{
  private static final int MIN_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;
  // A focused JMH comparison found that the cached-view mismatch path begins to outperform scalar reads at
  // 128 bytes.
  private static final int BYTE_BUFFER_MISMATCH_MIN_KEY_SIZE = 128;
  // The low word stores an encoded record location. A power-of-two records-per-page makes this the nonnegative
  // record number. Otherwise, the all-ones slot value is outside recordsPerPage. Therefore 0xffffffff is not a
  // valid location and can mark an empty bucket independently of the high-word metadata.
  private static final long EMPTY_REFERENCE = 0xFFFF_FFFFL;

  private final MergeMemoryLease memoryLease;
  private final KeySerde<KeyType> keySerde;
  private final int keySize;
  private final AggregatorAdapters aggregators;
  private final int configuredMaxSize;
  private final float maxLoadFactor;
  private final int configuredInitialBuckets;
  private final List<MergeMemoryPage> indexPageHandles = new ArrayList<>();
  private final List<MergeMemoryPage> payloadPageHandles = new ArrayList<>();
  private ByteBuffer[] indexPages = new ByteBuffer[0];
  private ByteBuffer[] payloadPages = new ByteBuffer[0];
  private ByteBuffer[] payloadComparisonViews = new ByteBuffer[0];
  private ByteBuffer firstIndexPage;
  private ByteBuffer firstPayloadPage;
  private int allocatedIndexPageCount;
  private int allocatedPayloadPageCount;
  private int buckets;
  private int slotsPerIndexPage;
  private int indexPageShift;
  private int indexPageMask;
  private int recordsPerPage;
  private int recordPageShift;
  private int recordPageMask;
  private int recordSize;
  private int size;
  private int growthCount;
  private int maxObservedSize;
  private long maxObservedUsedBytes;
  private boolean reachedCapacity;
  private boolean initialized;

  public PagedAggregationHashTable(
      final MergeMemoryLease memoryLease,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int configuredMaxSize,
      final float maxLoadFactor,
      final int initialBuckets
  )
  {
    this.memoryLease = memoryLease;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = aggregators;
    this.configuredMaxSize = configuredMaxSize;
    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.configuredInitialBuckets = initialBuckets > 0 ? Math.max(MIN_BUCKETS, initialBuckets) : DEFAULT_INITIAL_BUCKETS;
    if (this.maxLoadFactor <= 0 || this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be > 0 and < 1.0", this.maxLoadFactor);
    }
  }

  /** Compatibility constructor for focused tests and benchmarks. */
  public PagedAggregationHashTable(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int configuredMaxSize,
      final float maxLoadFactor,
      final int initialBuckets,
      final int pageSize
  )
  {
    this(new FixedBufferLease(bufferSupplier, pageSize), keySerde, aggregators, configuredMaxSize, maxLoadFactor, initialBuckets);
  }

  @Override
  public void init()
  {
    if (initialized) {
      return;
    }
    if (memoryLease.pageSize() < Long.BYTES || memoryLease.pageSize() % Long.BYTES != 0) {
      throw new IAE("Page size[%d] must be divisible by [%d]", memoryLease.pageSize(), Long.BYTES);
    }
    recordSize = Math.max(1, keySize + aggregators.spaceNeeded());
    recordsPerPage = memoryLease.pageSize() / recordSize;
    if (recordsPerPage == 0) {
      throw new IAE("Page size[%d] is too small for record size[%d]", memoryLease.pageSize(), recordSize);
    }
    recordPageShift = Integer.SIZE - Integer.numberOfLeadingZeros(recordsPerPage - 1);
    recordPageMask = recordPageShift == 0 ? 0 : -1 >>> (Integer.SIZE - recordPageShift);
    slotsPerIndexPage = memoryLease.pageSize() / Long.BYTES;
    indexPageShift = Integer.bitCount(slotsPerIndexPage) == 1
                     ? Integer.numberOfTrailingZeros(slotsPerIndexPage)
                     : -1;
    indexPageMask = slotsPerIndexPage - 1;
    initializeEmptyTable();
    initialized = true;
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(final KeyType key, final int keyHash)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (keyBuffer == null) {
      return Groupers.dictionaryFull(0);
    }
    if (keyBuffer.remaining() != keySize) {
      throw new IAE("keySerde remaining[%s] != keySize[%s]", keyBuffer.remaining(), keySize);
    }
    final int indexMetadata = keySize == Integer.BYTES
                              ? keyBuffer.getInt(keyBuffer.position())
                              : Groupers.getUsedFlag(keyHash);
    int slot;
    long reference;
    ByteBuffer page;
    int offset;
    int payloadPageIndex;
    boolean insertionCapacityEnsured = false;
    while (true) {
      slot = keyHash & (buckets - 1);
      page = null;
      offset = 0;
      while (true) {
        reference = getReference(slot);
        if (reference == EMPTY_REFERENCE) {
          break;
        }
        if (decodeIndexMetadata(reference) == indexMetadata) {
          payloadPageIndex = pageIndexForReference(reference);
          page = payloadPageIndex == 0 ? firstPayloadPage : payloadPages[payloadPageIndex];
          offset = offsetForReference(reference);
          if (keySize == Integer.BYTES || keysEqual(keyBuffer, page, payloadPageIndex, offset)) {
            break;
          }
        }
        slot = (slot + 1) & (buckets - 1);
      }
      if (reference != EMPTY_REFERENCE) {
        break;
      }
      if (insertionCapacityEnsured) {
        final int record = size;
        page = pageForRecord(record);
        offset = offsetForRecord(record);
        copyKey(keyBuffer, page, offset);
        aggregators.init(page, offset + keySize);
        putReference(slot, encodeReference(record, indexMetadata));
        size++;
        maxObservedSize = Math.max(maxObservedSize, size);
        break;
      }
      if (size >= configuredMaxSize || !ensureInsertionCapacity()) {
        reachedCapacity = true;
        return Groupers.hashTableFull(0);
      }
      insertionCapacityEnsured = true;
    }
    aggregators.aggregateBuffered(page, offset + keySize);
    return AggregateResult.ok();
  }

  @Override
  public void reset()
  {
    if (!initialized) {
      return;
    }
    releasePages();
    size = 0;
    buckets = 0;
    keySerde.reset();
    aggregators.reset();
    initializeEmptyTable();
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (!initialized) {
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }
    final int[] records = new int[size];
    for (int i = 0; i < size; i++) {
      records[i] = i;
    }
    if (sorted) {
      final BufferComparator comparator = keySerde.bufferComparator();
      IntArrays.quickSort(records, (left, right) -> comparator.compare(
          pageForRecord(left), pageForRecord(right), offsetForRecord(left), offsetForRecord(right)
      ));
    }
    return new CloseableIterator<>()
    {
      private final ReusableEntry<KeyType> reusableEntry = ReusableEntry.create(keySerde, aggregators.size());
      private int current;

      @Override
      public boolean hasNext()
      {
        return current < records.length;
      }

      @Override
      public Entry<KeyType> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return populateEntry(reusableEntry, records[current++]);
      }

      @Override
      public void close()
      {
        // Nothing to close.
      }
    };
  }

  @Override
  public void close()
  {
    releasePages();
    keySerde.reset();
    aggregators.reset();
  }

  @Override
  public long getMaxMergeBufferUsedBytes()
  {
    return maxObservedUsedBytes;
  }

  @Override
  public double getMaxSpillProximity()
  {
    if (reachedCapacity) {
      return 1.0;
    }
    return configuredMaxSize == 0 ? 0.0 : Math.min(1.0, (double) maxObservedSize / configuredMaxSize);
  }

  int getSize()
  {
    return size;
  }

  int getPageCount()
  {
    return allocatedPayloadPageCount;
  }

  int getGrowthCount()
  {
    return growthCount;
  }

  /**
   * Reserve enough space to import {@code entryCount} records without any further allocation. This is used by the
   * adaptive grouper so that promotion is all-or-nothing: no aggregator state is relocated until every required page
   * has been acquired.
   */
  boolean prepareForImport(final int entryCount)
  {
    if (!initialized || size != 0 || allocatedPayloadPageCount != 0) {
      throw new ISE("Can only prepare an initialized empty table for import");
    }
    if (entryCount < 0 || entryCount > configuredMaxSize) {
      return false;
    }

    int targetBuckets = buckets;
    while (entryCount >= Math.max(1, (int) (targetBuckets * maxLoadFactor))) {
      if (targetBuckets >= 1 << 30) {
        return false;
      }
      targetBuckets <<= 1;
    }

    final int requiredIndexPages = indexPageCount(targetBuckets);
    final int requiredPayloadPages = entryCount == 0 ? 0 : (entryCount - 1) / recordsPerPage + 1;
    final int additionalIndexPages = requiredIndexPages - allocatedIndexPageCount;
    final int pagesToAcquire = additionalIndexPages + requiredPayloadPages;
    final List<MergeMemoryPage> acquired;
    if (pagesToAcquire == 0) {
      acquired = Collections.emptyList();
    } else {
      final Optional<List<MergeMemoryPage>> pages = memoryLease.tryAcquirePages(pagesToAcquire);
      if (!pages.isPresent()) {
        return false;
      }
      acquired = pages.get();
    }

    if (additionalIndexPages > 0) {
      addIndexPages(acquired.subList(0, additionalIndexPages));
    }
    if (requiredPayloadPages > 0) {
      addPayloadPages(acquired.subList(additionalIndexPages, acquired.size()));
    }
    if (targetBuckets != buckets) {
      buckets = targetBuckets;
      growthCount++;
    }
    clearIndex();
    updateMaxObservedUsedBytes();
    return true;
  }

  /** Import one raw key and aggregation-state record after a successful {@link #prepareForImport(int)} call. */
  void importEntry(
      final ByteBuffer source,
      final int sourceKeyOffset,
      final int sourceAggregatorOffset,
      final int keyHash
  )
  {
    if (size >= configuredMaxSize || size >= allocatedPayloadPageCount * recordsPerPage) {
      throw new ISE("Paged table was not prepared for another imported entry");
    }
    final int record = size;
    final ByteBuffer target = pageForRecord(record);
    final int targetOffset = offsetForRecord(record);
    for (int i = 0; i < keySize; i++) {
      target.put(targetOffset + i, source.get(sourceKeyOffset + i));
    }
    for (int i = 0; i < aggregators.spaceNeeded(); i++) {
      target.put(targetOffset + keySize + i, source.get(sourceAggregatorOffset + i));
    }
    aggregators.relocate(sourceAggregatorOffset, targetOffset + keySize, source, target);

    final int indexMetadata = keySize == Integer.BYTES
                              ? target.getInt(targetOffset)
                              : Groupers.getUsedFlag(keyHash);
    putReference(findSlotForRebuild(keyHash), encodeReference(record, indexMetadata));
    size++;
    maxObservedSize = Math.max(maxObservedSize, size);
  }

  /** Release pages acquired by an unsuccessful promotion without resetting shared serde or aggregator state. */
  void abortImport()
  {
    if (size != 0) {
      throw new ISE("Cannot abort an import after records have been relocated");
    }
    releasePages();
    initialized = false;
  }

  public static int minimumPageCount(final int pageSize, final int configuredInitialBuckets)
  {
    if (pageSize < Long.BYTES || pageSize % Long.BYTES != 0) {
      throw new IAE("Page size[%d] must be divisible by [%d]", pageSize, Long.BYTES);
    }
    final int initialBuckets = configuredInitialBuckets > 0
                               ? Math.max(MIN_BUCKETS, configuredInitialBuckets)
                               : DEFAULT_INITIAL_BUCKETS;
    final int slotsPerPage = pageSize / Long.BYTES;
    return (nextPowerOfTwo(initialBuckets) + slotsPerPage - 1) / slotsPerPage + 1;
  }

  private void initializeEmptyTable()
  {
    final int initialBuckets = nextPowerOfTwo(configuredInitialBuckets);
    final int requiredPages = indexPageCount(initialBuckets);
    final Optional<List<MergeMemoryPage>> pages = memoryLease.tryAcquirePages(requiredPages);
    if (!pages.isPresent()) {
      throw new ISE("Unable to acquire [%d] initial index pages", requiredPages);
    }
    addIndexPages(pages.get());
    buckets = initialBuckets;
    clearIndex();
    updateMaxObservedUsedBytes();
  }

  private boolean ensureInsertionCapacity()
  {
    final boolean needsPayloadPage = size % recordsPerPage == 0;
    final int targetBuckets = size >= currentThreshold() ? buckets << 1 : buckets;
    if (targetBuckets <= 0) {
      return false;
    }
    final int additionalIndexPages = indexPageCount(targetBuckets) - allocatedIndexPageCount;
    final int requiredPages = additionalIndexPages + (needsPayloadPage ? 1 : 0);
    final List<MergeMemoryPage> acquired;
    if (requiredPages == 0) {
      acquired = Collections.emptyList();
    } else {
      final Optional<List<MergeMemoryPage>> pages = memoryLease.tryAcquirePages(requiredPages);
      if (!pages.isPresent()) {
        return false;
      }
      acquired = pages.get();
    }
    if (additionalIndexPages > 0) {
      addIndexPages(acquired.subList(0, additionalIndexPages));
    }
    if (needsPayloadPage) {
      addPayloadPages(Collections.singletonList(acquired.get(additionalIndexPages)));
    }
    if (targetBuckets != buckets) {
      buckets = targetBuckets;
      clearIndex();
      rebuildIndex();
      growthCount++;
    }
    if (requiredPages > 0) {
      updateMaxObservedUsedBytes();
    }
    return true;
  }

  private void rebuildIndex()
  {
    final KeyType key = keySerde.createKey();
    for (int record = 0; record < size; record++) {
      keySerde.readFromByteBuffer(key, pageForRecord(record), offsetForRecord(record));
      final int keyHash = hashFunction().applyAsInt(key);
      final int indexMetadata = keySize == Integer.BYTES
                                ? pageForRecord(record).getInt(offsetForRecord(record))
                                : Groupers.getUsedFlag(keyHash);
      putReference(findSlotForRebuild(keyHash), encodeReference(record, indexMetadata));
    }
  }

  private int findSlotForRebuild(final int keyHash)
  {
    int slot = keyHash & (buckets - 1);
    while (getReference(slot) != EMPTY_REFERENCE) {
      slot = (slot + 1) & (buckets - 1);
    }
    return slot;
  }

  private boolean keysEqual(
      final ByteBuffer key,
      final ByteBuffer page,
      final int payloadPageIndex,
      final int offset
  )
  {
    final int keyOffset = key.position();
    if (keySize == Integer.BYTES) {
      return key.getInt(keyOffset) == page.getInt(offset);
    }
    if (keySize == Long.BYTES) {
      return key.getLong(keyOffset) == page.getLong(offset);
    }
    if (keySize == 2 * Long.BYTES) {
      return key.getLong(keyOffset) == page.getLong(offset)
             && key.getLong(keyOffset + Long.BYTES) == page.getLong(offset + Long.BYTES);
    }
    if (keySize >= BYTE_BUFFER_MISMATCH_MIN_KEY_SIZE) {
      return keysEqualUsingMismatch(key, payloadComparisonViews[payloadPageIndex], offset);
    }
    return keysEqualScalar(key, page, keyOffset, offset);
  }

  private boolean keysEqualUsingMismatch(final ByteBuffer key, final ByteBuffer comparisonView, final int offset)
  {
    comparisonView.clear();
    comparisonView.position(offset);
    comparisonView.limit(offset + keySize);
    return key.mismatch(comparisonView) == -1;
  }

  private boolean keysEqualScalar(
      final ByteBuffer key,
      final ByteBuffer page,
      int keyOffset,
      int pageOffset
  )
  {
    int remaining = keySize;
    while (remaining >= Long.BYTES) {
      if (key.getLong(keyOffset) != page.getLong(pageOffset)) {
        return false;
      }
      keyOffset += Long.BYTES;
      pageOffset += Long.BYTES;
      remaining -= Long.BYTES;
    }
    if (remaining >= Integer.BYTES) {
      if (key.getInt(keyOffset) != page.getInt(pageOffset)) {
        return false;
      }
      keyOffset += Integer.BYTES;
      pageOffset += Integer.BYTES;
      remaining -= Integer.BYTES;
    }
    while (remaining-- > 0) {
      if (key.get(keyOffset++) != page.get(pageOffset++)) {
        return false;
      }
    }
    return true;
  }

  private long getReference(final int bucket)
  {
    if (bucket < slotsPerIndexPage) {
      return firstIndexPage.getLong(bucket * Long.BYTES);
    }
    if (indexPageShift >= 0) {
      return indexPages[bucket >>> indexPageShift].getLong((bucket & indexPageMask) * Long.BYTES);
    }
    return indexPages[bucket / slotsPerIndexPage].getLong((bucket % slotsPerIndexPage) * Long.BYTES);
  }

  private void putReference(final int bucket, final long reference)
  {
    if (bucket < slotsPerIndexPage) {
      firstIndexPage.putLong(bucket * Long.BYTES, reference);
      return;
    }
    if (indexPageShift >= 0) {
      indexPages[bucket >>> indexPageShift].putLong((bucket & indexPageMask) * Long.BYTES, reference);
    } else {
      indexPages[bucket / slotsPerIndexPage].putLong((bucket % slotsPerIndexPage) * Long.BYTES, reference);
    }
  }

  private void clearIndex()
  {
    for (int bucket = 0; bucket < buckets; bucket++) {
      putReference(bucket, EMPTY_REFERENCE);
    }
  }

  private void addIndexPages(final List<MergeMemoryPage> pages)
  {
    indexPages = ensureCapacity(indexPages, allocatedIndexPageCount + pages.size());
    for (final MergeMemoryPage page : pages) {
      indexPageHandles.add(page);
      final ByteBuffer indexPage = clear(page.get());
      indexPages[allocatedIndexPageCount] = indexPage;
      if (allocatedIndexPageCount == 0) {
        firstIndexPage = indexPage;
      }
      allocatedIndexPageCount++;
    }
  }

  private void addPayloadPages(final List<MergeMemoryPage> pages)
  {
    payloadPages = ensureCapacity(payloadPages, allocatedPayloadPageCount + pages.size());
    payloadComparisonViews = ensureCapacity(payloadComparisonViews, allocatedPayloadPageCount + pages.size());
    for (final MergeMemoryPage page : pages) {
      payloadPageHandles.add(page);
      final ByteBuffer payloadPage = clear(page.get());
      payloadPages[allocatedPayloadPageCount] = payloadPage;
      if (allocatedPayloadPageCount == 0) {
        firstPayloadPage = payloadPage;
      }
      payloadComparisonViews[allocatedPayloadPageCount++] = keySize >= BYTE_BUFFER_MISMATCH_MIN_KEY_SIZE
                                                            ? payloadPage.duplicate()
                                                            : payloadPage;
    }
  }

  private void releasePages()
  {
    indexPageHandles.forEach(MergeMemoryPage::close);
    payloadPageHandles.forEach(MergeMemoryPage::close);
    indexPageHandles.clear();
    payloadPageHandles.clear();
    Arrays.fill(indexPages, 0, allocatedIndexPageCount, null);
    Arrays.fill(payloadPages, 0, allocatedPayloadPageCount, null);
    Arrays.fill(payloadComparisonViews, 0, allocatedPayloadPageCount, null);
    allocatedIndexPageCount = 0;
    allocatedPayloadPageCount = 0;
    firstIndexPage = null;
    firstPayloadPage = null;
  }

  private int indexPageCount(final int bucketCount)
  {
    return (bucketCount + slotsPerIndexPage - 1) / slotsPerIndexPage;
  }

  private int currentThreshold()
  {
    return Math.max(1, (int) (buckets * maxLoadFactor));
  }

  private ByteBuffer pageForRecord(final int record)
  {
    if (record < recordsPerPage) {
      return firstPayloadPage;
    }
    return payloadPages[record / recordsPerPage];
  }

  private int offsetForRecord(final int record)
  {
    return (record % recordsPerPage) * recordSize;
  }

  private void copyKey(final ByteBuffer key, final ByteBuffer page, final int offset)
  {
    final int keyPosition = key.position();
    for (int i = 0; i < keySize; i++) {
      page.put(offset + i, key.get(keyPosition + i));
    }
  }

  private Entry<KeyType> populateEntry(final ReusableEntry<KeyType> entry, final int record)
  {
    final ByteBuffer page = pageForRecord(record);
    final int offset = offsetForRecord(record);
    keySerde.readFromByteBuffer(entry.getKey(), page, offset);
    if (entry.getValues().length != aggregators.size()) {
      throw new ISE("Expected entry with [%d] values but got [%d]", aggregators.size(), entry.getValues().length);
    }
    for (int i = 0; i < aggregators.size(); i++) {
      entry.getValues()[i] = aggregators.get(page, offset + keySize, i);
    }
    return entry;
  }

  private void updateMaxObservedUsedBytes()
  {
    maxObservedUsedBytes = Math.max(
        maxObservedUsedBytes,
        (long) (allocatedIndexPageCount + allocatedPayloadPageCount) * memoryLease.pageSize()
    );
  }

  private static ByteBuffer[] ensureCapacity(final ByteBuffer[] pages, final int requiredCapacity)
  {
    if (pages.length >= requiredCapacity) {
      return pages;
    }
    return Arrays.copyOf(pages, Math.max(requiredCapacity, Math.max(4, pages.length << 1)));
  }

  private static ByteBuffer clear(final ByteBuffer buffer)
  {
    final ByteBuffer duplicate = buffer.duplicate();
    duplicate.clear();
    return duplicate;
  }

  private static int nextPowerOfTwo(final int value)
  {
    if (value <= MIN_BUCKETS) {
      return MIN_BUCKETS;
    }
    final int highest = Integer.highestOneBit(value - 1);
    return highest > (1 << 29) ? 1 << 30 : highest << 1;
  }

  private long encodeReference(final int record, final int indexMetadata)
  {
    final long page = record / recordsPerPage;
    final long slotWithinPage = record % recordsPerPage;
    final long location = (page << recordPageShift) | slotWithinPage;
    return ((long) indexMetadata << Integer.SIZE) | (location & 0xFFFF_FFFFL);
  }

  private ByteBuffer pageForReference(final long reference)
  {
    return payloadPages[pageIndexForReference(reference)];
  }

  private int pageIndexForReference(final long reference)
  {
    return (int) reference >>> recordPageShift;
  }

  private int offsetForReference(final long reference)
  {
    return ((int) reference & recordPageMask) * recordSize;
  }

  private static int decodeIndexMetadata(final long reference)
  {
    return (int) (reference >>> Integer.SIZE);
  }

  private static class FixedBufferLease implements MergeMemoryLease
  {
    private final Supplier<ByteBuffer> bufferSupplier;
    private final int pageSize;
    private final Deque<ByteBuffer> pages = new ArrayDeque<>();
    private boolean initialized;

    private FixedBufferLease(final Supplier<ByteBuffer> bufferSupplier, final int pageSize)
    {
      if (pageSize <= 0) {
        throw new IAE("Invalid pageSize[%d], must be > 0", pageSize);
      }
      this.bufferSupplier = bufferSupplier;
      this.pageSize = pageSize;
    }

    @Override
    public synchronized Optional<List<MergeMemoryPage>> tryAcquirePages(final int count)
    {
      initialize();
      if (pages.size() < count) {
        return Optional.empty();
      }
      final List<MergeMemoryPage> result = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final ByteBuffer page = pages.removeFirst();
        result.add(new MergeMemoryPage()
        {
          private boolean closed;

          @Override
          public ByteBuffer get()
          {
            if (closed) {
              throw new ISE("Page is closed");
            }
            return page;
          }

          @Override
          public void close()
          {
            synchronized (FixedBufferLease.this) {
              if (!closed) {
                closed = true;
                pages.addLast(page);
              }
            }
          }
        });
      }
      return Optional.of(result);
    }

    @Override
    public int pageSize()
    {
      return pageSize;
    }

    @Override
    public void close()
    {
      pages.clear();
    }

    private void initialize()
    {
      if (initialized) {
        return;
      }
      final ByteBuffer buffer = bufferSupplier.get().duplicate();
      buffer.clear();
      for (int offset = 0; offset + pageSize <= buffer.capacity(); offset += pageSize) {
        final ByteBuffer page = buffer.duplicate();
        page.position(offset);
        page.limit(offset + pageSize);
        pages.addLast(page.slice());
      }
      initialized = true;
    }
  }
}
