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

package org.apache.druid.msq.querykit;

import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.RemoteDruidConnection;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class ExternalDataSourcePlannerTest
{
  private final ExternalDataSource external = new ExternalDataSource(
      new RemoteDruidInputSource(
          new RemoteDruidConnection(URI.create("https://source.example"), null, null, null, null, null),
          "events", null, null, null, false,
          new HttpInputSourceConfig(null, null),
          new DefaultObjectMapper()
      ),
      null,
      RowSignature.builder().add("__time", ColumnType.LONG).add("a", ColumnType.STRING).build()
  );

  @Test
  public void testTimeBoundsPushDownWithNonTimePredicate()
  {
    final DimFilter nonTime = new RangeFilter("a", ColumnType.STRING, "10", null, true, false, null);
    final DimFilter filter = new AndDimFilter(List.of(
        nonTime,
        new RangeFilter("__time", ColumnType.LONG, 1000L, 2000L, false, true, null)
    ));
    final RemoteDruidInputSource source = planned(filter);
    Assertions.assertEquals(List.of(Intervals.utc(1000, 2000)), source.getIntervals());
    Assertions.assertNull(source.getFilter());
    Assertions.assertNull(((RemoteDruidInputSource) external.getInputSource()).getIntervals());
    Assertions.assertNull(((RemoteDruidInputSource) external.getInputSource()).getFilter());
  }

  @Test
  public void testUnsupportedConjunctStaysLocal()
  {
    final DimFilter time = new RangeFilter("__time", ColumnType.LONG, 1000L, 2000L, false, true, null);
    final DimFilter expression = new ExpressionDimFilter("__time > 1500", ExprMacroTable.nil());
    final RemoteDruidInputSource source = planned(new AndDimFilter(List.of(time, expression)));
    Assertions.assertEquals(List.of(Intervals.utc(1000, 2000)), source.getIntervals());
    Assertions.assertNull(source.getFilter());
    Assertions.assertSame(external, plan(external, expression));
  }

  @Test
  public void testOrAndVirtualColumnsAreNotPartiallyPushed()
  {
    final DimFilter time = new RangeFilter("__time", ColumnType.LONG, 1000L, 2000L, false, true, null);
    final DimFilter virtual = new EqualityFilter("v0", ColumnType.STRING, "keep", null);
    Assertions.assertSame(external, plan(external, virtual));
    Assertions.assertSame(external, plan(external, new OrDimFilter(List.of(time, virtual))));
    Assertions.assertSame(external, plan(external, null));
  }

  @Test
  public void testNonLongTimeSignatureStaysLocal()
  {
    final ExternalDataSource coercedTime = new ExternalDataSource(
        external.getInputSource(),
        null,
        RowSignature.builder().add("__time", ColumnType.STRING).build()
    );
    Assertions.assertSame(coercedTime, plan(
        coercedTime,
        new EqualityFilter("__time", ColumnType.STRING, "1000", null)
    ));
  }

  private RemoteDruidInputSource planned(final DimFilter filter)
  {
    return (RemoteDruidInputSource) plan(external, filter).getInputSource();
  }
  @Test
  public void testTimeEqualityAndSelectorPushDown()
  {
    for (final DimFilter filter : List.of(
        new EqualityFilter("__time", ColumnType.LONG, 1500L, null),
        new SelectorDimFilter("__time", "1500", null)
    )) {
      final ExternalDataSource pushed = plan(external, filter);
      final RemoteDruidInputSource source = (RemoteDruidInputSource) pushed.getInputSource();
      Assertions.assertEquals(List.of(Intervals.utc(1500, 1501)), source.getIntervals());
      Assertions.assertNull(source.getFilter());
    }
    Assertions.assertSame(external, plan(
        external,
        new SelectorDimFilter("__time", "1500", IdentityExtractionFn.getInstance())
    ));
  }

  private ExternalDataSource plan(final ExternalDataSource dataSource, final DimFilter filter)
  {
    return (ExternalDataSource) DataSourcePlan.forDataSource(
        new QueryKitSpec(null, new DataSourcePlanners(Map.of()), "queryId"),
        QueryContext.empty(),
        dataSource,
        new MultipleIntervalSegmentSpec(List.of(Intervals.ETERNITY)),
        filter,
        0,
        false
    ).getNewDataSource();
  }

}
