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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SegmentsTableDataProviderTest
{
  @Test
  public void testPushdownFilters()
  {
    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> Mockito.mock(BrokerSegmentMetadataCache.class),
        Mockito.mock(MetadataSegmentView.class),
        new DefaultObjectMapper()
    );

    Assertions.assertEquals(
        Set.of("datasource", "segment_id", "start", "end", "version"),
        provider.getPushdownColumns()
    );
  }

  @Test
  public void testDatasourceFilterIsPushedIntoBothLocalViews()
  {
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments(Set.of("foo")))
           .thenReturn(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    Mockito.when(metadataCache.iterateSegmentMetadata(Set.of("foo"))).thenReturn(Collections.emptyIterator());

    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        (Provider<BrokerSegmentMetadataCache>) () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );
    final List<Filter> filters = List.of(new SelectorFilter("datasource", "foo"));

    Assertions.assertTrue(toRows(provider.getRows(filters)).isEmpty());
    Mockito.verify(metadataView).getSegments(Set.of("foo"));
    Mockito.verify(metadataCache).iterateSegmentMetadata(Set.of("foo"));
    Mockito.verify(metadataCache, Mockito.never()).getTotalSegments();
  }

  /**
   * Filters that allow no datasource return no rows without reading either view.
   */
  @Test
  public void testDisjointFiltersReadNoSegments()
  {
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );

    final List<Filter> filters = List.of(new SelectorFilter("datasource", "foo"), new SelectorFilter("datasource", "bar"));
    Assertions.assertTrue(toRows(provider.getRows(filters)).isEmpty());
    Mockito.verifyNoInteractions(metadataCache, metadataView);
  }

  /**
   * The Coordinator segment response closes once its rows are read, for callers that don't close the iterator.
   */
  @Test
  public void testCoordinatorSegmentsAreClosedOnceExhausted()
  {
    final AtomicBoolean closed = new AtomicBoolean();
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments(Set.of("foo")))
           .thenReturn(CloseableIterators.wrap(Collections.emptyIterator(), () -> closed.set(true)));
    Mockito.when(metadataCache.iterateSegmentMetadata(Set.of("foo"))).thenReturn(Collections.emptyIterator());

    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );
    final Iterable<Object[]> rows = provider.getRows(
        List.of(new SelectorFilter("datasource", "foo"))
    );

    Mockito.verify(metadataView, Mockito.never()).getSegments(ArgumentMatchers.any());
    Assertions.assertTrue(toRows(rows).isEmpty());
    Assertions.assertTrue(closed.get());
  }

  /**
   * Each pass tracks its own published segments, so a segment no longer published shows up as available.
   */
  @Test
  public void testEachPassTracksItsOwnPublishedSegments()
  {
    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v1", null)
    ).build();
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments((Set<String>) null))
           .thenReturn(
               CloseableIterators.withEmptyBaggage(
                   List.of(new SegmentStatusInCluster(segment, false, 1, 10L, false)).iterator()
               )
           )
           .thenReturn(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    Mockito.when(metadataCache.iterateSegmentMetadata(null)).thenAnswer(
        ignored -> List.of(
            AvailableSegmentMetadata.builder(segment, 0, Collections.emptySet(), null, 10).build()
        ).iterator()
    );
    Mockito.when(metadataCache.getTotalSegments()).thenReturn(1);

    final Iterable<Object[]> rows = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    ).getRows(Collections.emptyList());
    final int isPublished = SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("is_published");

    final List<Object[]> firstPass = toRows(rows);
    Assertions.assertEquals(1, firstPass.size());
    Assertions.assertEquals(1L, firstPass.get(0)[isPublished]);
    final List<Object[]> secondPass = toRows(rows);
    Assertions.assertEquals(1, secondPass.size());
    Assertions.assertEquals(0L, secondPass.get(0)[isPublished]);
  }

  @Test
  public void testExactSegmentFieldsAreFilteredBeforeRowsAreBuilt()
  {
    final DataSegment matchingSegment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v1", null)
    ).build();
    final DataSegment otherSegment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2001/2002"), "v2", null)
    ).build();
    final List<SegmentStatusInCluster> segments = List.of(
        new SegmentStatusInCluster(matchingSegment, false, 1, 10L, false),
        new SegmentStatusInCluster(otherSegment, false, 1, 10L, false)
    );
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments((Set<String>) null))
           .thenAnswer(ignored -> CloseableIterators.withEmptyBaggage(segments.iterator()));
    Mockito.when(metadataCache.iterateSegmentMetadata(null)).thenAnswer(ignored -> Collections.emptyIterator());
    Mockito.when(metadataCache.getTotalSegments()).thenReturn(segments.size());

    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );
    final Map<String, String> filters = Map.of(
        "segment_id", matchingSegment.getId().toString(),
        "start", matchingSegment.getInterval().getStart().toString(),
        "end", matchingSegment.getInterval().getEnd().toString(),
        "version", matchingSegment.getVersion()
    );

    for (final Map.Entry<String, String> filter : filters.entrySet()) {
      final List<Object[]> rows = toRows(
          provider.getRows(
              List.of(new SelectorFilter(filter.getKey(), filter.getValue()))
          )
      );
      Assertions.assertEquals(1, rows.size(), filter.getKey());
      Assertions.assertEquals(matchingSegment.getId().toString(), rows.get(0)[0], filter.getKey());
    }
  }

  @Test
  public void testExactSegmentFieldsFilterAvailableSegments()
  {
    final DataSegment matchingSegment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v1", null)
    ).build();
    final DataSegment otherSegment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2001/2002"), "v2", null)
    ).build();
    final List<AvailableSegmentMetadata> segments = List.of(
        AvailableSegmentMetadata.builder(matchingSegment, 0, Collections.emptySet(), null, 10).build(),
        AvailableSegmentMetadata.builder(otherSegment, 0, Collections.emptySet(), null, 10).build()
    );
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments((Set<String>) null))
           .thenAnswer(ignored -> CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    Mockito.when(metadataCache.iterateSegmentMetadata(null)).thenAnswer(ignored -> segments.iterator());
    Mockito.when(metadataCache.getTotalSegments()).thenReturn(segments.size());

    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );
    final List<Object[]> rows = toRows(
        provider.getRows(
            List.of(new SelectorFilter("segment_id", matchingSegment.getId().toString()))
        )
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(matchingSegment.getId().toString(), rows.get(0)[0]);
  }

  @Test
  public void testComplexColumnsUseBindableJsonRepresentation()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v", null)
    ).dimensions(List.of("dim1"))
                            .metrics(List.of("metric1"))
                            .build();
    final Object[] rawRow = new Object[SegmentsTableDescriptor.ROW_SIGNATURE.size()];
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("segment_id")] = segment.getId();
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = segment.getDataSource();
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("shard_spec")] = segment.getShardSpec();
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("dimensions")] = segment.getDimensions();
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("metrics")] = segment.getMetrics();

    final Object[] row = SegmentsTableDataProvider.projectRow(rawRow, null, mapper);

    Assertions.assertEquals(segment.getId().toString(), row[0]);
    Assertions.assertEquals("[\"dim1\"]", row[15]);
    Assertions.assertEquals("[\"metric1\"]", row[16]);
  }

  /**
   * A query that reads only some columns gets rows holding those columns and {@code datasource}, which rows are
   * authorized by, so JSON columns it doesn't read aren't serialized.
   */
  @Test
  public void testRowsHoldOnlyQueriedColumnsAndDatasource() throws Exception
  {
    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v1", null)
    ).dimensions(List.of("dim1")).build();
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    final List<SegmentStatusInCluster> segments = List.of(new SegmentStatusInCluster(segment, false, 1, 10L, false));
    Mockito.when(metadataView.getSegments((Set<String>) null))
           .thenAnswer(ignored -> CloseableIterators.withEmptyBaggage(segments.iterator()));
    Mockito.when(metadataCache.iterateSegmentMetadata(null)).thenAnswer(ignored -> Collections.emptyIterator());
    final ObjectMapper mapper = Mockito.mock(ObjectMapper.class);

    final List<Object[]> rows = toRows(
        new SegmentsTableDataProvider(() -> metadataCache, metadataView, mapper)
            .getRows(Collections.emptyList(), Set.of("segment_id"))
    );

    final Object[] expectedRow = new Object[SegmentsTableDescriptor.ROW_SIGNATURE.size()];
    expectedRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("segment_id")] = segment.getId().toString();
    expectedRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = "foo";
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(expectedRow, rows.get(0));
    Mockito.verify(mapper, Mockito.never()).writeValueAsString(Mockito.any());
  }

  /**
   * Closing the rows' iterator before reading them all, as a query with a limit does, closes the Coordinator response.
   */
  @Test
  public void testClosingRowsEarlyClosesCoordinatorSegments() throws Exception
  {
    final AtomicBoolean closed = new AtomicBoolean();
    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2000/2001"), "v1", null)
    ).build();
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    final List<SegmentStatusInCluster> segments = List.of(
        new SegmentStatusInCluster(segment, false, 1, 10L, false),
        new SegmentStatusInCluster(segment, false, 1, 10L, false)
    );
    Mockito.when(metadataView.getSegments((Set<String>) null))
           .thenReturn(CloseableIterators.wrap(segments.iterator(), () -> closed.set(true)));
    Mockito.when(metadataCache.iterateSegmentMetadata(null)).thenAnswer(ignored -> Collections.emptyIterator());

    final Iterator<Object[]> rows = new SegmentsTableDataProvider(
        () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    ).getRows(Collections.emptyList()).iterator();
    rows.next();
    Assertions.assertFalse(closed.get());

    ((Closeable) rows).close();
    Assertions.assertTrue(closed.get());
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new java.util.ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
