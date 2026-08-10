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

package org.apache.druid.indexing.overlord.http;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class NativeTasksTableSupplierTest
{
  @Test
  public void testExtractsExactDataSourceFromAndFilter()
  {
    final ScanQuery query = scanQuery(
        new AndDimFilter(
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new LikeDimFilter("task_id", "native_sys_mvp_%", null, null)
        )
    );

    final List<DimFilter> extracted = extract(query);

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertInstanceOf(SelectorDimFilter.class, extracted.get(0));
    Assertions.assertEquals("native_sys_a", NativeTasksTableSupplier.toDataSourceFilter(extracted));
  }

  @Test
  public void testPushesMultipleDataSourcesFromOrFilter()
  {
    final ScanQuery query = scanQuery(
        new OrDimFilter(
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new SelectorDimFilter("datasource", "native_sys_b", null)
        )
    );

    final List<DimFilter> extracted = extract(query);

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals(
        Set.of("native_sys_a", "native_sys_b"),
        NativeTasksTableSupplier.toTaskStorageQueryFilter(extracted).getDataSources()
    );
  }

  @Test
  public void testExtractsNativeEqualityFilter()
  {
    final EqualityFilter filter = new EqualityFilter("datasource", ColumnType.STRING, "native_sys_a", null);

    final List<DimFilter> extracted = extract(scanQuery(filter));

    Assertions.assertEquals(List.of(filter), extracted);
    Assertions.assertEquals("native_sys_a", NativeTasksTableSupplier.toDataSourceFilter(extracted));
  }

  @Test
  public void testExtractsDataSourceFromWindowLeafScan()
  {
    final ScanQuery scanQuery = scanQuery(new SelectorDimFilter("datasource", "native_sys_a", null));
    final WindowOperatorQuery windowQuery = new WindowOperatorQuery(
        new QueryDataSource(scanQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        Collections.emptyMap(),
        RowSignature.empty(),
        Collections.emptyList(),
        null
    );

    final List<DimFilter> extracted = extract(windowQuery);

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals("native_sys_a", NativeTasksTableSupplier.toDataSourceFilter(extracted));
  }

  @Test
  public void testConvertsAllStorageBackedTaskFilters()
  {
    final ScanQuery query = scanQuery(
        new AndDimFilter(
            new TypedInFilter("task_id", ColumnType.STRING, List.of("task-a", "task-b"), null, null),
            new SelectorDimFilter("group_id", "group-a", null),
            new SelectorDimFilter("type", "noop", null),
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new EqualityFilter("created_time", ColumnType.STRING, "2026-01-02T00:00:00.000Z", null),
            new RangeFilter(
                "created_time",
                ColumnType.STRING,
                "2026-01-01T00:00:00.000Z",
                "2026-02-01T00:00:00.000Z",
                false,
                true,
                null
            ),
            new SelectorDimFilter("status", "SUCCESS", null),
            new LikeDimFilter("error_msg", "%ignored%", null, null)
        )
    );

    final List<DimFilter> extracted = extract(query);
    final TaskStorageQueryFilter storageFilter = NativeTasksTableSupplier.toTaskStorageQueryFilter(extracted);

    Assertions.assertEquals(7, extracted.size());
    Assertions.assertEquals(Set.of("task-a", "task-b"), storageFilter.getTaskIds());
    Assertions.assertEquals(Set.of("group-a"), storageFilter.getGroupIds());
    Assertions.assertEquals(Set.of("noop"), storageFilter.getTypes());
    Assertions.assertEquals(Set.of("native_sys_a"), storageFilter.getDataSources());
    Assertions.assertEquals(Set.of("2026-01-02T00:00:00.000Z"), storageFilter.getCreatedTimes());
    Assertions.assertEquals("2026-01-01T00:00:00.000Z", storageFilter.getCreatedTimeLower());
    Assertions.assertEquals("2026-02-01T00:00:00.000Z", storageFilter.getCreatedTimeUpper());
    Assertions.assertFalse(storageFilter.isCreatedTimeLowerOpen());
    Assertions.assertTrue(storageFilter.isCreatedTimeUpperOpen());
    Assertions.assertFalse(storageFilter.includesActiveTasks());
    Assertions.assertTrue(storageFilter.includesCompleteTasks());
  }

  private static List<DimFilter> extract(final Query<?> query)
  {
    final NativeTasksTableSupplier dataSupplier = new NativeTasksTableSupplier(null, null);
    return NativeSystemTableFilterExtractor.extract(query, dataSupplier.getFilterRules());
  }

  private static ScanQuery scanQuery(final DimFilter filter)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource(NativeTasksTableSupplier.TABLE_NAME))
                 .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                 .filters(filter)
                 .build();
  }
}
