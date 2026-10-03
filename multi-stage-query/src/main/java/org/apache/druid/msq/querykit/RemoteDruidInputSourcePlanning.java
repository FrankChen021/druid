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

import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Narrow remote time intervals, retaining the original target-side filter. */
public class RemoteDruidInputSourcePlanning
{
  private RemoteDruidInputSourcePlanning()
  {
  }

  public static DataSource pushDown(final DataSource dataSource, @Nullable final DimFilter filter)
  {
    if (!(dataSource instanceof ExternalDataSource external)
        || !(external.getInputSource() instanceof RemoteDruidInputSource remote) || filter == null
        || !ColumnType.LONG.equals(external.getSignature().getColumnType("__time").orElse(null))) {
      return dataSource;
    }
    final List<DimFilter> accepted = new ArrayList<>();
    collect(filter, accepted);
    if (accepted.isEmpty()) {
      return dataSource;
    }
    final Filtration filtration = Filtration.create(new AndDimFilter(accepted)).optimize(external.getSignature());
    return new ExternalDataSource(
        remote.withReadFilter(filtration.getDimFilter(), filtration.getIntervals()),
        external.getInputFormat(),
        external.getSignature()
    );
  }

  private static void collect(final DimFilter filter, final List<DimFilter> accepted)
  {
    // Other columns may be coerced by EXTERN; filtering them on the source could exclude matching target rows.
    if (filter instanceof AndDimFilter and) {
      for (final DimFilter child : and.getFields()) {
        collect(child, accepted);
      }
    } else if (Set.of("__time").equals(filter.getRequiredColumns())
               && (filter instanceof EqualityFilter || filter instanceof RangeFilter
                   || (filter instanceof SelectorDimFilter selector && selector.getExtractionFn() == null))) {
      if (filter instanceof EqualityFilter equality && ColumnType.LONG.equals(equality.getMatchValueType())) {
        accepted.add(new RangeFilter(
            "__time",
            ColumnType.LONG,
            equality.getMatchValue(),
            equality.getMatchValue(),
            false,
            false,
            null
        ));
      } else if (filter instanceof SelectorDimFilter selector && selector.getValue() != null) {
        try {
          final long value = Long.parseLong(selector.getValue());
          accepted.add(new RangeFilter("__time", ColumnType.LONG, value, value, false, false, null));
        }
        catch (NumberFormatException e) {
          accepted.add(filter);
        }
      } else {
        accepted.add(filter);
      }
    }
  }
}
