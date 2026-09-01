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

public class BatchedInlineDataSourceTest
{
  @Test
  public void testVectorCursorLoadsMultipleBatches()
  {
    final RowSignature signature = RowSignature.builder()
                                               .add("value", ColumnType.LONG)
                                               .build();
    final List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < BatchedInlineDataSource.BATCH_SIZE + 1; i++) {
      rows.add(new Object[]{(long) i});
    }

    final CursorFactory cursorFactory = makeCursorFactory(new BatchedInlineDataSource(rows, signature));
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertTrue(cursorHolder.canVectorize());
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      final VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("value");
      Assertions.assertEquals(BatchedInlineDataSource.BATCH_SIZE, cursor.getCurrentVectorSize());
      Assertions.assertEquals(0L, selector.getLongVector()[0]);
      Assertions.assertEquals(
          BatchedInlineDataSource.BATCH_SIZE - 1L,
          selector.getLongVector()[BatchedInlineDataSource.BATCH_SIZE - 1]
      );

      cursor.advance();

      Assertions.assertEquals(1, cursor.getCurrentVectorSize());
      Assertions.assertEquals(BatchedInlineDataSource.BATCH_SIZE, selector.getLongVector()[0]);
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

  private static CursorFactory makeCursorFactory(final BatchedInlineDataSource dataSource)
  {
    final Segment segment = new BatchedInlineDataSource.Wrangler()
        .getSegmentsForIntervals(dataSource, List.<Interval>of())
        .iterator()
        .next();
    return segment.as(CursorFactory.class);
  }
}
