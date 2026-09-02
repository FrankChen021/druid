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

package org.apache.druid.query;

import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BatchedInlineDataSourceTest
{
  @Test
  public void testVectorCursorLoadsMultipleBatches()
  {
    final int batchSize = QueryContexts.DEFAULT_VECTOR_SIZE;
    final RowSignature signature = RowSignature.builder()
                                               .add("value", ColumnType.LONG)
                                               .build();
    final List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < batchSize + 1; i++) {
      rows.add(new Object[]{(long) i});
    }

    final CursorFactory cursorFactory = makeCursorFactory(new BatchedInlineDataSource(rows, signature));
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertTrue(cursorHolder.canVectorize());
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      final VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("value");
      Assertions.assertEquals(batchSize, cursor.getCurrentVectorSize());
      Assertions.assertEquals(0L, selector.getLongVector()[0]);
      Assertions.assertEquals(
          batchSize - 1L,
          selector.getLongVector()[batchSize - 1]
      );

      cursor.advance();

      Assertions.assertEquals(1, cursor.getCurrentVectorSize());
      Assertions.assertEquals(batchSize, selector.getLongVector()[0]);
      cursor.advance();
      Assertions.assertTrue(cursor.isDone());
    }
  }

  @Test
  public void testUnsupportedTypeUsesRowCursor()
  {
    final RowSignature signature = RowSignature.builder()
                                               .add("value", ColumnType.DOUBLE)
                                               .build();
    final CursorFactory cursorFactory = makeCursorFactory(
        new BatchedInlineDataSource(List.<Object[]>of(new Object[]{1.5D}), signature)
    );

    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertFalse(cursorHolder.canVectorize());
      Assertions.assertNotNull(cursorHolder.asCursor());
    }
  }

  @Test
  public void testUsesConfiguredVectorSize()
  {
    final RowSignature signature = RowSignature.builder()
                                               .add("value", ColumnType.LONG)
                                               .build();
    final CursorFactory cursorFactory = makeCursorFactory(
        new BatchedInlineDataSource(
            List.of(
                new Object[]{1L},
                new Object[]{2L},
                new Object[]{3L},
                new Object[]{4L},
                new Object[]{5L}
            ),
            signature
        )
    );
    final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                .setQueryContext(
                                                    QueryContext.of(Map.of(QueryContexts.VECTOR_SIZE_KEY, 4))
                                                )
                                                .build();

    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(spec)) {
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      Assertions.assertEquals(4, cursor.getMaxVectorSize());
      Assertions.assertEquals(4, cursor.getCurrentVectorSize());
      cursor.advance();
      Assertions.assertEquals(1, cursor.getCurrentVectorSize());
    }
  }

  private static CursorFactory makeCursorFactory(final BatchedInlineDataSource dataSource)
  {
    final Segment segment = new BatchedInlineDataSource.Wrangler()
        .getSegmentsForIntervals(dataSource, List.<Interval>of())
        .iterator()
        .next();
    return segment.as(CursorFactory.class);
  }
}
