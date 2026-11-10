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

package org.apache.druid.server.system.table;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SystemTableVectorCursorTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("datasource", ColumnType.STRING)
                                                            .add("size", ColumnType.LONG)
                                                            .build();
  private static final List<Object[]> ROWS = List.of(
      new Object[]{"allowed", 1L},
      new Object[]{"secret", 2L},
      new Object[]{"allowed", null},
      new Object[]{"allowed", 4}
  );

  /**
   * A query with a filter reads rows one at a time, which applies the filter; other queries can vectorize.
   */
  @Test
  public void testVectorizesOnlyQueriesWithoutFilters()
  {
    final Filter filter = new EqualityFilter("datasource", ColumnType.STRING, "allowed", null);
    final CursorBuildSpec aggregation = CursorBuildSpec.builder()
                                                       .setAggregators(List.of(new LongSumAggregatorFactory("s", "size")))
                                                       .build();

    try (CursorHolder holder = cursorFactory(SystemTableAccess.ALL_ROWS).makeCursorHolder(aggregation)) {
      Assertions.assertTrue(holder.canVectorize());
    }
    try (CursorHolder holder = cursorFactory(SystemTableAccess.ALL_ROWS).makeCursorHolder(
        CursorBuildSpec.builder(aggregation).setFilter(filter).build()
    )) {
      Assertions.assertFalse(holder.canVectorize());
    }
  }

  /**
   * The cursor reads only the rows the caller may read, in batches of the query's vector size, and reads numbers and
   * nulls of long columns.
   */
  @Test
  public void testReadsAuthorizedRowsInBatches()
  {
    final SystemTableAccess access = new SystemTableAccess(row -> "allowed".equals(row[0]));

    final List<List<Object>> batches = new ArrayList<>();
    try (CursorHolder holder = cursorFactory(access).makeCursorHolder(spec(2, null))) {
      final VectorCursor cursor = holder.asVectorCursor();
      final VectorObjectSelector datasource = cursor.getColumnSelectorFactory().makeObjectSelector("datasource");
      final VectorValueSelector size = cursor.getColumnSelectorFactory().makeValueSelector("size");
      while (!cursor.isDone()) {
        final List<Object> batch = new ArrayList<>();
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          batch.add(datasource.getObjectVector()[i]);
          batch.add(size.getNullVector()[i] ? null : size.getLongVector()[i]);
        }
        batches.add(batch);
        cursor.advance();
      }
    }

    Assertions.assertEquals(
        List.of(Arrays.asList("allowed", 1L, "allowed", null), List.of("allowed", 4L)),
        batches
    );
  }

  /**
   * Columns the query doesn't read aren't copied into the batches, and read as nulls.
   */
  @Test
  public void testReadsOnlyQueriedColumns()
  {
    try (CursorHolder holder = cursorFactory(SystemTableAccess.ALL_ROWS).makeCursorHolder(spec(4, Set.of("size")))) {
      final VectorCursor cursor = holder.asVectorCursor();
      final VectorObjectSelector datasource = cursor.getColumnSelectorFactory().makeObjectSelector("datasource");
      final VectorValueSelector size = cursor.getColumnSelectorFactory().makeValueSelector("size");

      Assertions.assertEquals(4, cursor.getCurrentVectorSize());
      Assertions.assertArrayEquals(new Object[4], datasource.getObjectVector());
      Assertions.assertEquals(2L, size.getLongVector()[1]);
    }
  }

  /**
   * Resetting the cursor after it advanced reads the rows again; resetting it on its first batch doesn't.
   */
  @Test
  public void testResetReadsRowsAgainOnlyAfterAdvancing()
  {
    final AtomicInteger reads = new AtomicInteger();
    final SystemTableDataProvider provider = filters -> () -> {
      reads.incrementAndGet();
      return ROWS.iterator();
    };

    try (CursorHolder holder = new SystemTableCursorFactory(provider, SIGNATURE, SystemTableAccess.ALL_ROWS)
        .makeCursorHolder(spec(2, null))) {
      final VectorCursor cursor = holder.asVectorCursor();
      cursor.reset();
      Assertions.assertEquals(1, reads.get());

      cursor.advance();
      cursor.reset();
      Assertions.assertEquals(2, reads.get());
      Assertions.assertEquals(2, cursor.getCurrentVectorSize());
    }
  }

  /**
   * Closing the cursor holder before reading every row, as a query with a limit does, closes the provider's rows.
   */
  @Test
  public void testClosesProviderIteratorWhenQueryStopsEarly()
  {
    final AtomicBoolean closed = new AtomicBoolean();
    final SystemTableDataProvider provider = filters -> () -> CloseableIterators.wrap(
        ROWS.iterator(),
        () -> closed.set(true)
    );

    try (CursorHolder holder = new SystemTableCursorFactory(provider, SIGNATURE, SystemTableAccess.ALL_ROWS)
        .makeCursorHolder(spec(1, null))) {
      Assertions.assertFalse(holder.asVectorCursor().isDone());
      Assertions.assertFalse(closed.get());
    }

    Assertions.assertTrue(closed.get());
  }

  @Test
  public void testRejectsValueOfWrongType()
  {
    final SystemTableDataProvider provider = filters -> List.<Object[]>of(new Object[]{"allowed", "not a number"});

    try (CursorHolder holder = new SystemTableCursorFactory(provider, SIGNATURE, SystemTableAccess.ALL_ROWS)
        .makeCursorHolder(spec(2, null))) {
      final DruidException e = Assertions.assertThrows(DruidException.class, holder::asVectorCursor);
      Assertions.assertEquals("Column[size] of type[LONG] has a value of class[java.lang.String]", e.getMessage());
    }
  }

  private static SystemTableCursorFactory cursorFactory(final SystemTableAccess access)
  {
    return new SystemTableCursorFactory(filters -> ROWS, SIGNATURE, access);
  }

  private static CursorBuildSpec spec(final int vectorSize, @Nullable final Set<String> physicalColumns)
  {
    return CursorBuildSpec.builder()
                          .setPhysicalColumns(physicalColumns)
                          .setQueryContext(
                              QueryContext.of(QueryContext.ofMap(QueryContextParameters.VECTOR_SIZE, vectorSize))
                          )
                          .build();
  }
}
