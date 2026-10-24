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

package org.apache.druid.server.system;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Druids;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.QueriesTableDataProvider;
import org.apache.druid.server.system.table.QueriesTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.apache.druid.server.system.table.SystemTableQueryInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class QueriesTableDataProviderTest
{
  @Test
  public void testMapsRowsFromAllLocalProviders()
  {
    final SystemTableQueryInfoProvider nativeProvider = () -> List.of(
        new SystemTableQueryInfo(
            "native-1",
            "native",
            "RUNNING",
            Map.of("queryType", "scan"),
            "sql-1",
            false,
            "historical:8083",
            "historical"
        )
    );
    final SystemTableQueryInfoProvider dartProvider = () -> List.of(
        new SystemTableQueryInfo(
            "dart-1",
            "msq-dart",
            "RUNNING",
            Map.of(),
            "sql-1",
            true,
            "broker:8082",
            "broker"
        )
    );
    final QueriesTableDataProvider provider = new QueriesTableDataProvider(Set.of(nativeProvider, dartProvider), new DefaultObjectMapper());

    final List<Object[]> rows = StreamSupport.stream(provider.getRows(List.of()).spliterator(), false).toList();

    Assertions.assertEquals(2, rows.size());
    Assertions.assertTrue(
        rows.stream().anyMatch(
            row -> "native-1".equals(row[0])
                   && "{\"queryType\":\"scan\"}".equals(row[3])
                   && "sql-1".equals(row[4])
                   && Long.valueOf(0).equals(row[5])
                   && "historical:8083".equals(row[6])
                   && "historical".equals(row[7])
        )
    );
    Assertions.assertTrue(
        rows.stream().anyMatch(
            row -> "dart-1".equals(row[0])
                   && "sql-1".equals(row[4])
                   && Long.valueOf(1).equals(row[5])
                   && "broker:8082".equals(row[6])
                   && "broker".equals(row[7])
        )
    );
  }

  @Test
  public void testRowAuthorizerRequiresStateRead()
  {
    final Authorizer denyState = (authenticationResult, resource, action) ->
        ResourceType.STATE.equals(resource.getType()) ? Access.DENIED : Access.OK;
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(Map.of("denyState", denyState));
    final AuthenticationResult caller = new AuthenticationResult("caller", "denyState", null, null);

    final ForbiddenException e = Assertions.assertThrows(
        ForbiddenException.class,
        () -> new QueriesTableDescriptor().getRowAuthorizer().filterAuthorizedRows(List.of(), caller, authorizerMapper)
    );
    Assertions.assertTrue(e.getMessage().startsWith("Insufficient permission to view queries"), e.getMessage());
  }

  /** Equality filters on each pushed-down column skip non-matching rows; a column with a null value never matches. */
  @ParameterizedTest(name = "{0} = {1}")
  @CsvSource({
      "id, native-2, native-2",
      "engine, msq-dart, dart-1",
      "initial_query_id, sql-1, native-1|dart-1",
      "server, broker:8082, dart-1|native-2",
      "server_type, historical, native-1",
      "initial_query_id, missing, ''"
  })
  public void testPushesDownEqualityFilter(final String column, final String value, final String expectedIds)
  {
    Assertions.assertEquals(
        expectedIds.isEmpty() ? Set.of() : Set.of(expectedIds.split("\\|")),
        rowIds(new EqualityFilter(column, ColumnType.STRING, value, null))
    );
  }

  /** Filters on one column intersect, and IN filters push down like equality filters. */
  @Test
  public void testIntersectsFiltersOnTheSameColumn()
  {
    Assertions.assertEquals(
        Set.of("native-1", "native-2"),
        rowIds(new InDimFilter("engine", Set.of("native", "msq-task"), null))
    );
    Assertions.assertEquals(
        Set.of("native-2"),
        rowIds(
            new InDimFilter("engine", Set.of("native", "msq-dart"), null),
            new SelectorDimFilter("server", "broker:8082", null),
            new SelectorDimFilter("engine", "native", null)
        )
    );
  }

  /** Other filter types aren't applied by the provider; the native scan applies them after the rows are returned. */
  @Test
  public void testIgnoresOtherFilterTypes()
  {
    Assertions.assertEquals(
        Set.of("native-1", "dart-1", "native-2"),
        rowIds(new LikeDimFilter("id", "native%", null, null))
    );
  }

  /** The filters a native scan pushes down for the provider's columns are the ones the provider applies. */
  @Test
  public void testAppliesFiltersExtractedFromQuery()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(QueriesTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("initial_query_id", ColumnType.STRING, "sql-1", null),
                                          new EqualityFilter("state", ColumnType.STRING, "RUNNING", null)
                                      )
                                  )
                                  .build();
    final List<DimFilter> pushedFilters =
        SystemTablePushdownFilter.extract(query, provider().getPushdownFilters());

    Assertions.assertEquals(
        List.of(new EqualityFilter("initial_query_id", ColumnType.STRING, "sql-1", null)),
        pushedFilters
    );
    Assertions.assertEquals(Set.of("native-1", "dart-1"), rowIds(pushedFilters.toArray(new DimFilter[0])));
  }

  private static Set<Object> rowIds(final DimFilter... filters)
  {
    return StreamSupport.stream(provider().getRows(List.of(filters)).spliterator(), false)
                        .map(row -> row[0])
                        .collect(Collectors.toSet());
  }

  private static QueriesTableDataProvider provider()
  {
    final SystemTableQueryInfoProvider queryInfoProvider = () -> List.of(
        new SystemTableQueryInfo("native-1", "native", "RUNNING", null, "sql-1", false, "historical:8083", "historical"),
        new SystemTableQueryInfo("dart-1", "msq-dart", "RUNNING", null, "sql-1", true, "broker:8082", "broker"),
        new SystemTableQueryInfo("native-2", "native", "RUNNING", null, "sql-2", true, "broker:8082", "broker")
    );
    return new QueriesTableDataProvider(Set.of(queryInfoProvider), new DefaultObjectMapper());
  }
}
