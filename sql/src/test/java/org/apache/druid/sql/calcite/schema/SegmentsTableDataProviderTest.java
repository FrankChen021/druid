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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableQueryRequest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SegmentsTableDataProviderTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  @Test
  public void testDatasourceFilterIsPushedIntoBothLocalViews()
  {
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    final MetadataSegmentView metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments(Set.of("foo"))).thenReturn(Collections.emptyIterator());
    Mockito.when(metadataCache.iterateSegmentMetadata(Set.of("foo"))).thenReturn(Collections.emptyIterator());

    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        (Provider<BrokerSegmentMetadataCache>) () -> metadataCache,
        metadataView,
        new DefaultObjectMapper()
    );
    final List<DimFilter> filters = List.of(new SelectorDimFilter("datasource", "foo", null));

    Assertions.assertTrue(toRows(provider.getRows(filters, AUTHENTICATION_RESULT)).isEmpty());
    Mockito.verify(metadataView).getSegments(Set.of("foo"));
    Mockito.verify(metadataCache).iterateSegmentMetadata(Set.of("foo"));
    Mockito.verify(metadataCache, Mockito.never()).getTotalSegments();
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

  @Test
  public void testProjectionSkipsUnrequestedJsonColumns() throws Exception
  {
    final ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> Mockito.mock(BrokerSegmentMetadataCache.class),
        Mockito.mock(MetadataSegmentView.class),
        mapper
    );
    final Object[] rawRow = new Object[SegmentsTableDescriptor.ROW_SIGNATURE.size()];
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = "foo";
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("dimensions")] = List.of("unused");

    final Object[] projectedRow = provider.projectRow(
        rawRow,
        new int[]{SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")}
    );

    Assertions.assertArrayEquals(new Object[]{"foo"}, projectedRow);
    Mockito.verify(mapper, Mockito.never()).writeValueAsString(Mockito.any());
  }

  @Test
  public void testAuthorizedDataSourceProjectsRowsIntoBatches() throws Exception
  {
    final ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
    final SegmentsTableDataProvider provider = new SegmentsTableDataProvider(
        () -> Mockito.mock(BrokerSegmentMetadataCache.class),
        Mockito.mock(MetadataSegmentView.class),
        mapper
    );
    final Object[] rawRow = new Object[SegmentsTableDescriptor.ROW_SIGNATURE.size()];
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("datasource")] = "foo";
    rawRow[SegmentsTableDescriptor.ROW_SIGNATURE.indexOf("dimensions")] = List.of("unused");
    final RowSignature projectedSignature = RowSignature.builder()
                                                        .add("datasource", ColumnType.STRING)
                                                        .build();

    final DataSource dataSource = provider.getAuthorizedDataSource(
        new SystemTableQueryRequest(
            List.of("datasource"),
            projectedSignature
        ),
        List.<Object[]>of(rawRow)
    ).orElseThrow();

    final BatchedInlineDataSource batchedDataSource = Assertions.assertInstanceOf(
        BatchedInlineDataSource.class,
        dataSource
    );
    Assertions.assertEquals(projectedSignature, batchedDataSource.getRowSignature());
    Assertions.assertArrayEquals(new Object[]{"foo"}, batchedDataSource.getRows().iterator().next());
    Mockito.verify(mapper, Mockito.never()).writeValueAsString(Mockito.any());
  }

  private static List<Object[]> toRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> result = new java.util.ArrayList<>();
    rows.forEach(result::add);
    return result;
  }
}
