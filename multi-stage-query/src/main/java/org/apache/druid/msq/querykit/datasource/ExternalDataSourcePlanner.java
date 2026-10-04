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

package org.apache.druid.msq.querykit.datasource;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Planner for {@link ExternalDataSource}.
 */
public class ExternalDataSourcePlanner implements DataSourcePlanner<ExternalDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final ExternalDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    DataSourcePlannerUtils.checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);

    return new DataSourcePlan(
        dataSource,
        Collections.singletonList(
            new ExternalInputSpec(
                dataSource.getInputSource(),
                dataSource.getInputFormat(),
                dataSource.getSignature()
            )
        ),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  /// Optimizes eligible external input predicates before constructing the datasource and its input specification.
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final ExternalDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      @Nullable final DimFilter filter,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    return planDataSource(
        queryKitSpec,
        queryContext,
        optimize(dataSource, filter),
        querySegmentSpec,
        minStageNumber,
        broadcast
    );
  }

  /// Optimizes an input source for the supplied filter.
  /// Currently applies eligible `__time` predicates to remote Druid input sources; other sources are unchanged.
  /// The caller must retain the original filter for evaluation on the target cluster.
  private static ExternalDataSource optimize(final ExternalDataSource dataSource, @Nullable final DimFilter filter)
  {
    if (!(dataSource.getInputSource() instanceof RemoteDruidInputSource remote) || filter == null
        || !ColumnType.LONG.equals(dataSource.getSignature().getColumnType("__time").orElse(null))) {
      return dataSource;
    }
    final List<DimFilter> accepted = new ArrayList<>();
    collect(filter, accepted);
    if (accepted.isEmpty()) {
      return dataSource;
    }
    final Filtration filtration = Filtration.create(new AndDimFilter(accepted)).optimize(dataSource.getSignature());
    return new ExternalDataSource(
        remote.withReadFilter(filtration.getDimFilter(), filtration.getIntervals()),
        dataSource.getInputFormat(),
        dataSource.getSignature()
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
