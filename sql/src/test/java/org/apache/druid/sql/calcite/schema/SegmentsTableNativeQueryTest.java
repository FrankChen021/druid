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

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.OperatorSequence;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.SegmentToRowsAndColumnsOperator;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableSegmentWrangler;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs native queries on {@code sys.segments} the way the Broker reads it: through {@link SystemTableSegmentWrangler},
 * with the access that authorizing the query attaches.
 */
public class SegmentsTableNativeQueryTest
{
  private static final String TABLE = SegmentsTableDescriptor.TABLE_NAME;
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", "test", null, null);
  private static final DataSegment ALLOWED_SEGMENT_1 = segment("allowed", "2000/2001");
  private static final DataSegment ALLOWED_SEGMENT_2 = segment("allowed", "2001/2002");
  private static final DataSegment DENIED_SEGMENT = segment("denied", "2000/2001");

  private final AtomicBoolean coordinatorResponseClosed = new AtomicBoolean();
  private MetadataSegmentView metadataView;
  private SystemTableSegmentWrangler wrangler;

  @BeforeEach
  public void setUp()
  {
    // Published in descending start order so that ordering by start is observable.
    final List<SegmentStatusInCluster> publishedSegments = List.of(
        new SegmentStatusInCluster(ALLOWED_SEGMENT_2, false, 1, 10L, false),
        new SegmentStatusInCluster(DENIED_SEGMENT, false, 1, 10L, false),
        new SegmentStatusInCluster(ALLOWED_SEGMENT_1, false, 1, 10L, false)
    );
    metadataView = Mockito.mock(MetadataSegmentView.class);
    Mockito.when(metadataView.getSegments(ArgumentMatchers.any())).thenAnswer(
        ignored -> CloseableIterators.wrap(publishedSegments.iterator(), () -> coordinatorResponseClosed.set(true))
    );
    final BrokerSegmentMetadataCache metadataCache = Mockito.mock(BrokerSegmentMetadataCache.class);
    Mockito.when(metadataCache.iterateSegmentMetadata(ArgumentMatchers.any()))
           .thenAnswer(ignored -> Collections.emptyIterator());
    Mockito.when(metadataCache.getTotalSegments()).thenReturn(publishedSegments.size());
    wrangler = new SystemTableSegmentWrangler(
        Map.of(TABLE, new SegmentsTableDataProvider(() -> metadataCache, metadataView, new DefaultObjectMapper())),
        Map.of(TABLE, new SegmentsTableDescriptor())
    );
  }

  /**
   * A caller without DATASOURCE READ on a datasource sees none of its segments, but still sees the others.
   */
  @Test
  public void testDeniedDatasourceSegmentsAreNotReturned()
  {
    final ScanQuery query = scanQuery(List.of("segment_id", "datasource"));

    final List<Object[]> rows = new ArrayList<>();
    runScan(query).forEach(
        result -> ((List<List<?>>) result.getEvents()).forEach(row -> rows.add(row.toArray()))
    );

    Assertions.assertEquals(2, rows.size());
    Assertions.assertArrayEquals(new Object[]{ALLOWED_SEGMENT_2.getId().toString(), "allowed"}, rows.get(0));
    Assertions.assertArrayEquals(new Object[]{ALLOWED_SEGMENT_1.getId().toString(), "allowed"}, rows.get(1));
  }

  /**
   * With the metadata cache disabled, the Coordinator response is read while the query runs. A query that stops
   * early, for example because of a LIMIT, must still release the response when its results are closed.
   */
  @Test
  public void testCoordinatorResponseIsReleasedWhenClosedEarly() throws Exception
  {
    final ScanQuery query = Druids.ScanQueryBuilder.copy(scanQuery(List.of("datasource"))).batchSize(1).build();

    final Yielder<ScanResultValue> yielder = Yielders.each(runScan(query));
    Assertions.assertEquals(1, ((List<?>) yielder.get().getEvents()).size());
    Assertions.assertFalse(coordinatorResponseClosed.get());
    yielder.close();

    Assertions.assertTrue(coordinatorResponseClosed.get());
  }

  /**
   * SELECT datasource, ROW_NUMBER() OVER (ORDER BY start) FROM sys.segments
   */
  @Test
  public void testWindowFunction()
  {
    final RowSignature outputSignature = RowSignature.builder()
                                                     .add("datasource", ColumnType.STRING)
                                                     .add("start", ColumnType.STRING)
                                                     .add("rn", ColumnType.LONG)
                                                     .build();
    final WindowOperatorQuery query = new WindowOperatorQuery(
        authorizedDataSource(),
        new LegacySegmentSpec(Intervals.ETERNITY),
        Collections.emptyMap(),
        outputSignature,
        List.of(
            new NaiveSortOperatorFactory(List.of(ColumnWithDirection.ascending("start"))),
            new NaivePartitioningOperatorFactory(List.of()),
            new WindowOperatorFactory(new WindowRowNumberProcessor("rn"))
        ),
        List.of(new ScanOperatorFactory(null, null, null, List.of("datasource", "start"), null, null))
    );

    final List<RowsAndColumns> results = runWindowOperators(query).toList();

    Assertions.assertEquals(1, results.size());
    final RowsAndColumns rac = results.get(0);
    final ColumnAccessor datasource = rac.findColumn("datasource").toAccessor();
    final ColumnAccessor start = rac.findColumn("start").toAccessor();
    final ColumnAccessor rowNumber = rac.findColumn("rn").toAccessor();
    Assertions.assertEquals(2, rac.numRows());
    for (int i = 0; i < rac.numRows(); i++) {
      Assertions.assertEquals("allowed", datasource.getObject(i));
      Assertions.assertEquals(i + 1L, rowNumber.getLong(i));
    }
    Assertions.assertEquals(ALLOWED_SEGMENT_1.getInterval().getStart().toString(), start.getObject(0));
    Assertions.assertEquals(ALLOWED_SEGMENT_2.getInterval().getStart().toString(), start.getObject(1));
    Mockito.verify(metadataView, Mockito.times(1)).getSegments(ArgumentMatchers.any());
  }

  private Sequence<ScanResultValue> runScan(final ScanQuery query)
  {
    final ResponseContext responseContext = ResponseContext.createEmpty();
    responseContext.putTimeoutTime(JodaUtils.MAX_INSTANT);
    return new ScanQueryEngine().process(query, toSegment(query.getDataSource()), responseContext, null);
  }

  private Sequence<RowsAndColumns> runWindowOperators(final WindowOperatorQuery query)
  {
    Operator operator = new SegmentToRowsAndColumnsOperator(toSegment(query.getDataSource()));
    for (final OperatorFactory factory : query.getLeafOperators()) {
      operator = factory.wrap(operator);
    }
    for (final OperatorFactory factory : query.getOperators()) {
      operator = factory.wrap(operator);
    }
    final Operator finalOperator = operator;
    return new OperatorSequence(() -> finalOperator);
  }

  private Segment toSegment(final DataSource dataSource)
  {
    return wrangler.getSegmentsForIntervals(dataSource, List.of(Intervals.ETERNITY)).iterator().next();
  }

  private static ScanQuery scanQuery(final List<String> columns)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(authorizedDataSource())
                 .eternityInterval()
                 .columns(columns)
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .build();
  }

  /**
   * Returns the table with the access that authorizing the query attaches.
   */
  private static SystemTableDataSource authorizedDataSource()
  {
    final SystemTableAccess access = new SegmentsTableDescriptor().getRowAuthorizer()
                                                                  .authorize(AUTHENTICATION_RESULT, authorizerMapper());
    return new SystemTableDataSource(TABLE).withAccess(access);
  }

  private static AuthorizerMapper authorizerMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            "denied".equals(resource.getName()) ? Access.DENIED : Access.OK;
      }
    };
  }

  private static DataSegment segment(final String dataSource, final String interval)
  {
    return DataSegment.builder(SegmentId.of(dataSource, Intervals.of(interval), "v1", null)).size(1L).build();
  }
}
