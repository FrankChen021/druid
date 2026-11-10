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
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableStringFilters;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * Native row supplier for {@code sys.segments}.
 */
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
  private static final Set<String> PUSHDOWN_COLUMNS = Set.of("datasource", "segment_id", "start", "end", "version");
  private static final int DATASOURCE_COLUMN = SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource");

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
  public Set<String> getPushdownColumns()
  {
    return PUSHDOWN_COLUMNS;
  }

  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters)
  {
    return getRows(filters, null);
  }

  /**
   * Returns rows that hold only the given columns, and {@code datasource}, which rows are authorized by. The other
   * columns are null, so the query doesn't pay for serializing JSON columns it doesn't read. Each pass over the rows
   * fetches the published segments again, and its iterator closes the Coordinator response when closed.
   */
  @Override
  public Iterable<Object[]> getRows(final List<Filter> filters, @Nullable final Set<String> columns)
  {
    final Map<String, Set<String>> stringValues = SystemTableStringFilters.getStringValues(filters);
    final int[] projects = getProjects(columns);
    return () -> {
      final Closer closer = Closer.create();
      final Iterator<Object[]> rawRows = Iterators.filter(
          getRawRowsIterator(
              segmentMetadataCacheProvider.get(),
              metadataView,
              stringValues.get("datasource"),
              segment -> matchesStringValues(segment, stringValues),
              closer
          ),
          Objects::nonNull
      );
      return CloseableIterators.wrap(Iterators.transform(rawRows, row -> fillRow(row, projects, jsonMapper)), closer);
    };
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

  /**
   * Returns one pass over the rows. Each pass tracks the published segments it has already returned.
   */
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

  /**
   * Converts raw segment fields to the strings declared by {@link SegmentsTableDescriptor#ROW_SIGNATURE}.
   */
  static Object[] projectRow(
      final Object[] row,
      @Nullable final int[] projects,
      final ObjectMapper jsonMapper
  )
  {
    final int[] nonNullProjects = projects == null ? PROJECT_ALL : projects;
    final Object[] projectedRow = new Object[nonNullProjects.length];
    for (int i = 0; i < nonNullProjects.length; i++) {
      projectedRow[i] = convertValue(nonNullProjects[i], row[nonNullProjects[i]], jsonMapper);
    }
    return projectedRow;
  }

  /**
   * Converts a raw value of the given column to the type declared by {@link SegmentsTableDescriptor#ROW_SIGNATURE}.
   */
  @Nullable
  private static Object convertValue(final int column, @Nullable final Object value, final ObjectMapper jsonMapper)
  {
    if (value == null
        || value instanceof String
        || !SegmentsTableDescriptor.ROW_SIGNATURE.getColumnType(column).get().is(ValueType.STRING)) {
      return value;
    } else if (!JSON_FIELDS.contains(column)) {
      return value.toString();
    }
    try {
      return jsonMapper.writeValueAsString(value);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(
          e,
          "Could not serialize column[%s] of system table[%s]",
          SegmentsTableDescriptor.ROW_SIGNATURE.getColumnName(column),
          SegmentsTableDescriptor.TABLE_NAME
      );
    }
  }

  /**
   * Returns a full-width row holding the given columns of the raw row, converted to their declared types. The other
   * columns are null.
   */
  private static Object[] fillRow(final Object[] row, final int[] projects, final ObjectMapper jsonMapper)
  {
    final Object[] filledRow = new Object[row.length];
    for (final int column : projects) {
      filledRow[column] = convertValue(column, row[column], jsonMapper);
    }
    return filledRow;
  }

  /**
   * Returns the columns to fill for a query that reads the given columns, or every column if null. The
   * {@code datasource} column is always filled, since rows are authorized by it.
   */
  private static int[] getProjects(@Nullable final Set<String> columns)
  {
    if (columns == null) {
      return PROJECT_ALL;
    }
    final IntSet projects = new IntOpenHashSet();
    projects.add(DATASOURCE_COLUMN);
    for (final String column : columns) {
      final int columnNumber = SegmentsTableDescriptor.ROW_SIGNATURE.indexOf(column);
      if (columnNumber >= 0) {
        projects.add(columnNumber);
      }
    }
    return projects.toIntArray();
  }

  private static boolean matchesStringValues(final DataSegment segment, final Map<String, Set<String>> stringValues)
  {
    return SystemTableStringFilters.matches(stringValues.get("datasource"), segment.getDataSource())
           && SystemTableStringFilters.matches(stringValues.get("segment_id"), segment.getId().toString())
           && SystemTableStringFilters.matches(stringValues.get("start"), segment.getInterval().getStart().toString())
           && SystemTableStringFilters.matches(stringValues.get("end"), segment.getInterval().getEnd().toString())
           && SystemTableStringFilters.matches(stringValues.get("version"), segment.getVersion());
  }

  /**
   * Closes the wrapped iterator once it is exhausted, or when closed first. Closing more than once is a no-op.
   */
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
