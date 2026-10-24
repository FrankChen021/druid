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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NativeSysSegmentsQueryTest extends QueryTestBase
{
  private String dataSource;

  @Override
  public void beforeAll()
  {
    dataSource = ingestBasicData();
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeSegmentAggregations(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*), COUNT(DISTINCT segment_id), COUNT(DISTINCT datasource), SUM(1)
        FROM sys.segments
        WHERE datasource = '%s'
        """,
        nativeQueryContext(plannerStrategy),
        dataSource
    );

    Assertions.assertEquals("10,10,1,10", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeNestedSegmentAggregation(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*) FROM (
          SELECT segment_id FROM sys.segments WHERE datasource = '%s' GROUP BY segment_id
        )
        """,
        nativeQueryContext(plannerStrategy),
        dataSource
    );

    Assertions.assertEquals("10", result);
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeWindowFunction(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT datasource, ROW_NUMBER() OVER (ORDER BY "start")
        FROM sys.segments
        WHERE datasource = '%s'
        """,
        nativeQueryContext(plannerStrategy),
        dataSource
    );

    final String expected = IntStream.rangeClosed(1, 10)
                                     .mapToObj(rowNumber -> dataSource + "," + rowNumber)
                                     .collect(Collectors.joining("\n"));
    Assertions.assertEquals(expected, result);
  }

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_QUERY_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
