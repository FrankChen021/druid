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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.SystemTableQueryRequest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/** Native row supplier for {@code sys.segments}. */
public class SegmentsTableDataProvider implements SystemTableDataProvider
{
  private static final long REPLICATION_FACTOR_UNKNOWN = -1L;
  private static final long IS_ACTIVE_FALSE = 0L;
  private static final long IS_ACTIVE_TRUE = 1L;
  private static final long IS_PUBLISHED_FALSE = 0L;
  private static final long IS_PUBLISHED_TRUE = 1L;
  private static final long IS_AVAILABLE_TRUE = 1L;
  private static final long IS_OVERSHADOWED_FALSE = 0L;
  private static final long IS_OVERSHADOWED_TRUE = 1L;

  private static final int[] PROJECT_ALL = IntStream.range(0, SegmentsTableDescriptor.ROW_SIGNATURE.size()).toArray();
  private static final IntSet JSON_FIELDS = new IntOpenHashSet(
      new int[]{
          SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("shard_spec"),
          SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("dimensions"),
          SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("metrics"),
          SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("projections"),
          SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("last_compaction_state")
      }
  );
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("datasource", null),
      new SystemTablePushdownFilter("segment_id", null),
      new SystemTablePushdownFilter("start", null),
      new SystemTablePushdownFilter("end", null),
      new SystemTablePushdownFilter("version", null)
  );

  private final Provider<BrokerSegmentMetadataCache> segmentMetadataCacheProvider;
  private final MetadataSegmentView metadataView;
  private final ObjectMapper jsonMapper;

  @Inject
  public SegmentsTableDataProvider(
      final Provider<BrokerSegmentMetadataCache> segmentMetadataCacheProvider,
      final MetadataSegmentView metadataView,
      final ObjectMapper jsonMapper
  )
  {
    this.segmentMetadataCacheProvider = segmentMetadataCacheProvider;
    this.metadataView = metadataView;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Optional<DataSource> getAuthorizedDataSource(
      final SystemTableQueryRequest request,
      final Iterable<Object[]> authorizedRows
  )
  {
    final int[] projects = request.columns()
                                  .stream()
                                  .mapToInt(SegmentsTableDescriptor.ROW_SIGNATURE::indexOf)
                                  .toArray();
    final Iterable<Object[]> projectedRows = Iterables.transform(
        authorizedRows,
        row -> projectRow(row, projects, jsonMapper)
    );
    return Optional.of(new BatchedInlineDataSource(projectedRows, request.rowSignature()));
  }

  /**
   * Returns rows that close the Coordinator response once they are read to the end. Native queries read
   * {@link #getRawRows(List, Closer)} instead, which also releases the response on early close.
   */
  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters)
  {
    return Iterables.transform(
        getFilteredRawRows(filters, Closer.create()),
        row -> projectRow(row, null, jsonMapper)
    );
  }

  /**
   * Returns single-pass rows: a second pass would fetch the published segments from the Coordinator again, so it
   * fails instead.
   */
  @Override
  public Iterable<Object[]> getRawRows(final List<DimFilter> filters, final Closer closer)
  {
    final Iterable<Object[]> rows = getFilteredRawRows(filters, closer);
    final AtomicBoolean read = new AtomicBoolean();
    return () -> {
      if (read.getAndSet(true)) {
        throw DruidException.defensive(
            "Rows of system table[%s] can only be read once per query",
            SegmentsTableDescriptor.TABLE_NAME
        );
      }
      return rows.iterator();
    };
  }

  private Iterable<Object[]> getFilteredRawRows(final List<DimFilter> filters, final Closer closer)
  {
    final Map<String, Set<String>> stringFilters = getStringFilters(filters);
    return Iterables.filter(
        getRawRows(
            segmentMetadataCacheProvider.get(),
            metadataView,
            stringFilters.get("datasource"),
            segment -> matchesStringFilters(segment, stringFilters),
            closer
        ),
        Objects::nonNull
    );
  }

  @Override
  public Object[] projectRow(final Object[] row, @Nullable final int[] projects)
  {
    return projectRow(row, projects, jsonMapper);
  }

  /**
   * Returns the unprojected rows shared by the native provider and the Bindable system-table implementation.
   * Each pass fetches published segments on its first read, and registers the response with {@code closer}.
   */
  static Iterable<Object[]> getRawRows(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final MetadataSegmentView metadataView,
      @Nullable final Set<String> dataSourceFilter,
      final Closer closer
  )
  {
    return getRawRows(segmentMetadataCache, metadataView, dataSourceFilter, segment -> true, closer);
  }

  private static Iterable<Object[]> getRawRows(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final MetadataSegmentView metadataView,
      @Nullable final Set<String> dataSourceFilter,
      final Predicate<DataSegment> segmentFilter,
      final Closer closer
  )
  {
    return () -> getRawRowsIterator(segmentMetadataCache, metadataView, dataSourceFilter, segmentFilter, closer);
  }

  /** Returns one pass over the rows. Each pass tracks the published segments it has already returned. */
  private static Iterator<Object[]> getRawRowsIterator(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final MetadataSegmentView metadataView,
      @Nullable final Set<String> dataSourceFilter,
      final Predicate<DataSegment> segmentFilter,
      final Closer closer
  )
  {
    final Set<SegmentId> segmentsAlreadySeen = dataSourceFilter == null
                                                ? Sets.newHashSetWithExpectedSize(
                                                    segmentMetadataCache.getTotalSegments()
                                                )
                                                : new HashSet<>();

    final FluentIterable<Object[]> publishedSegments = FluentIterable
        .from(() -> closer.register(new CloseWhenExhaustedIterator<>(metadataView.getSegments(dataSourceFilter))))
        .filter(val -> segmentFilter.test(val.getDataSegment()))
        .transform(val -> {
          final DataSegment segment = val.getDataSegment();
          final AvailableSegmentMetadata availableSegmentMetadata =
              segmentMetadataCache.getAvailableSegmentMetadata(segment.getDataSource(), segment.getId());
          segmentsAlreadySeen.add(segment.getId());

          long numReplicas = 0L;
          long isAvailable = 0L;
          if (availableSegmentMetadata != null) {
            numReplicas = availableSegmentMetadata.getNumReplicas();
            isAvailable = availableSegmentMetadata.getNumReplicas() > 0 ? IS_AVAILABLE_TRUE : IS_ACTIVE_FALSE;
          }

          final long numRows;
          if (segment.getTotalRows() != null) {
            numRows = segment.getTotalRows().longValue();
          } else if (val.getNumRows() != null) {
            numRows = val.getNumRows();
          } else if (availableSegmentMetadata != null) {
            numRows = availableSegmentMetadata.getNumRows();
          } else {
            numRows = 0L;
          }

          final long isRealtime = val.isRealtime() ? 1 : 0;
          final boolean isPublished = !val.isRealtime();
          final boolean isActive = isPublished ? !val.isOvershadowed() : val.isRealtime();

          return new Object[]{
              segment.getId(),
              segment.getDataSource(),
              segment.getInterval().getStart(),
              segment.getInterval().getEnd(),
              segment.getSize(),
              segment.getVersion(),
              (long) segment.getShardSpec().getPartitionNum(),
              numReplicas,
              numRows,
              isActive ? IS_ACTIVE_TRUE : IS_ACTIVE_FALSE,
              isPublished ? IS_PUBLISHED_TRUE : IS_PUBLISHED_FALSE,
              isAvailable,
              isRealtime,
              val.isOvershadowed() ? IS_OVERSHADOWED_TRUE : IS_OVERSHADOWED_FALSE,
              segment.getShardSpec(),
              segment.getDimensions(),
              segment.getMetrics(),
              segment.getProjections(),
              segment.getLastCompactionState(),
              val.getReplicationFactor() == null ? REPLICATION_FACTOR_UNKNOWN : (long) val.getReplicationFactor()
          };
        });

    final FluentIterable<Object[]> availableSegments = FluentIterable
        .from(() -> segmentMetadataCache.iterateSegmentMetadata(dataSourceFilter))
        .filter(val -> segmentFilter.test(val.getSegment()))
        .transform(val -> {
          final DataSegment segment = val.getSegment();
          if (segmentsAlreadySeen.contains(segment.getId())) {
            return null;
          }
          return new Object[]{
              segment.getId(),
              segment.getDataSource(),
              segment.getInterval().getStart(),
              segment.getInterval().getEnd(),
              segment.getSize(),
              segment.getVersion(),
              (long) segment.getShardSpec().getPartitionNum(),
              val.getNumReplicas(),
              segment.getTotalRows() != null ? segment.getTotalRows() : val.getNumRows(),
              val.isRealtime(),
              IS_PUBLISHED_FALSE,
              IS_AVAILABLE_TRUE,
              val.isRealtime(),
              IS_OVERSHADOWED_FALSE,
              segment.getShardSpec(),
              segment.getDimensions(),
              segment.getMetrics(),
              segment.getProjections(),
              null,
              REPLICATION_FACTOR_UNKNOWN
          };
        });

    return Iterators.unmodifiableIterator(Iterables.concat(publishedSegments, availableSegments).iterator());
  }

  /** Converts raw segment fields to the strings declared by {@link SegmentsTableDescriptor#ROW_SIGNATURE}. */
  static Object[] projectRow(
      final Object[] row,
      @Nullable final int[] projects,
      final ObjectMapper jsonMapper
  )
  {
    final int[] nonNullProjects = projects == null ? PROJECT_ALL : projects;
    final Object[] projectedRow = new Object[nonNullProjects.length];

    for (int i = 0; i < nonNullProjects.length; i++) {
      final int column = nonNullProjects[i];
      final Object value = row[column];
      if (SegmentsTableDescriptor.ROW_SIGNATURE.getColumnType(column).get().is(ValueType.STRING)
          && value != null
          && !(value instanceof String)) {
        if (JSON_FIELDS.contains(column)) {
          try {
            projectedRow[i] = jsonMapper.writeValueAsString(value);
          }
          catch (JsonProcessingException e) {
            throw DruidException.defensive(
                e,
                "Could not serialize column[%s] of system table[%s]",
                SegmentsTableDescriptor.ROW_SIGNATURE.getColumnName(column),
                SegmentsTableDescriptor.TABLE_NAME
            );
          }
        } else {
          projectedRow[i] = value.toString();
        }
      } else {
        projectedRow[i] = value;
      }
    }
    return projectedRow;
  }

  private static Map<String, Set<String>> getStringFilters(final List<DimFilter> filters)
  {
    final Map<String, Set<String>> result = new HashMap<>();
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter)) {
        final String column = SystemTablePushdownFilter.getStringValuesColumn(filter);
        final Set<String> values = SystemTablePushdownFilter.getStringValues(filter);
        result.compute(
            column,
            (ignored, currentValues) -> currentValues == null
                                        ? values
                                        : Sets.intersection(currentValues, values).immutableCopy()
        );
      }
    }
    return result;
  }

  private static boolean matchesStringFilters(
      final DataSegment segment,
      final Map<String, Set<String>> filters
  )
  {
    return matches(filters.get("datasource"), segment.getDataSource())
           && matches(filters.get("segment_id"), segment.getId().toString())
           && matches(filters.get("start"), segment.getInterval().getStart().toString())
           && matches(filters.get("end"), segment.getInterval().getEnd().toString())
           && matches(filters.get("version"), segment.getVersion());
  }

  private static boolean matches(@Nullable final Set<String> acceptedValues, final String value)
  {
    return acceptedValues == null || acceptedValues.contains(value);
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    if (filter instanceof SelectorDimFilter
        || filter instanceof EqualityFilter
        || filter instanceof InDimFilter
        || filter instanceof TypedInFilter) {
      return true;
    }
    if (filter instanceof OrDimFilter or) {
      return !or.getFields().isEmpty() && or.getFields().stream().allMatch(SegmentsTableDataProvider::isStringValuesFilter);
    }
    return false;
  }

  /** Closes the wrapped iterator once it is exhausted, or when closed first. Closing more than once is a no-op. */
  private static class CloseWhenExhaustedIterator<T> implements CloseableIterator<T>
  {
    private final CloseableIterator<T> delegate;
    private boolean closed;

    CloseWhenExhaustedIterator(final CloseableIterator<T> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext()
    {
      if (closed) {
        return false;
      }
      if (delegate.hasNext()) {
        return true;
      }
      CloseableUtils.closeAndWrapExceptions(this);
      return false;
    }

    @Override
    public T next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return delegate.next();
    }

    @Override
    public void close() throws IOException
    {
      if (!closed) {
        closed = true;
        delegate.close();
      }
    }
  }
}
