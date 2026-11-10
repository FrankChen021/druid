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

package org.apache.druid.testing.embedded.query.sys.segments;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.apache.druid.testing.embedded.query.QueryTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NativeSysSegmentsQueryTest extends QueryTestBase
{
  private String dataSource;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return super.createCluster().addCommonProperty("druid.query.enableDruidEngineForSystemTables", "true");
  }

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

  /**
   * A filter on a column that the query doesn't select still sees that column's values, though the provider fills only
   * the columns the query reads.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeFilterOnUnselectedColumn(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT segment_id
        FROM sys.segments
        WHERE datasource = '%s' AND is_published = 1 AND shard_spec IS NOT NULL
        """,
        nativeQueryContext(plannerStrategy),
        dataSource
    );

    Assertions.assertEquals(10, result.split("\n").length, result);
  }

  /**
   * Only Brokers serve {@code sys.segments}, so the Router forwards a native query on it to a Broker, which reads its
   * own rows.
   */
  @Test
  public void testNativeQueryThroughRouterAndBroker() throws Exception
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(SegmentsTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .filters(new EqualityFilter("datasource", ColumnType.STRING, dataSource, null))
                                  .columns("segment_id")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    for (final EmbeddedDruidServer<?> server : List.of(router, broker)) {
      final StatusResponseHolder response = HttpUtil.makeRequest(
          httpClientRef,
          HttpMethod.POST,
          getServerUrl(server) + "/druid/v2/",
          query,
          HttpResponseStatus.OK
      );
      final List<ScanResultValue> results = broker.bindings().jsonMapper().readValue(
          response.getContent(),
          new TypeReference<>() {}
      );
      final int rows = results.stream().mapToInt(result -> ((List<?>) result.getEvents()).size()).sum();
      Assertions.assertEquals(10, rows, server.toString());
    }
  }

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
