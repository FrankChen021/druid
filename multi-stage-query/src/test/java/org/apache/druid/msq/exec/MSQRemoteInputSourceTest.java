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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Runs target MSQ ingestion against a loopback source whose SQL API executes the pushed-down SQL with a real Druid
 * SQL planner and engine, then streams it back the way {@code /druid/v2/sql} does for {@code resultFormat: arrayLines}.
 */
public class MSQRemoteInputSourceTest extends MSQTestBase
{
  private final List<JsonNode> nativeRequests = new CopyOnWriteArrayList<>();
  private final List<JsonNode> sqlRequests = new CopyOnWriteArrayList<>();
  // Recorded by the source and checked on the test thread, since a failed assertion in a handler only breaks the
  // connection.
  private final List<String> authorizations = new CopyOnWriteArrayList<>();
  private volatile String sourceDefaultTimeZone = "UTC";
  private volatile String expectedAuthorization;
  private HttpServer server;

  @BeforeEach
  public void startSource() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        authorizations.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
        if ("DELETE".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(202, -1);
        } else if ("/druid/v2/sql".equals(exchange.getRequestURI().getPath())) {
          handleSql(exchange);
        } else {
          handleNative(exchange);
        }
      }
    });
    server.start();
  }

  @AfterEach
  public void stopSource()
  {
    server.stop(0);
    Assertions.assertFalse(authorizations.isEmpty());
    for (final String authorization : authorizations) {
      Assertions.assertEquals(String.valueOf(expectedAuthorization), authorization);
    }
  }

  private void handleNative(final HttpExchange exchange) throws IOException
  {
    final JsonNode request = objectMapper.readTree(exchange.getRequestBody());
    nativeRequests.add(request);
    final Query<?> parsed = objectMapper.treeToValue(request, Query.class);
    // Expose the fixture's three primitive columns in native schema discovery.
    final Query<?> query = parsed instanceof SegmentMetadataQuery metadata
                           ? metadata.withColumns(new ListColumnIncluderator(List.of("__time", "dim1", "cnt")))
                           : parsed;
    final List<?> result = QueryPlus.wrap(query).run(queryFramework().walker(), ResponseContext.createEmpty()).toList();
    final byte[] response = objectMapper.writeValueAsBytes(result);
    exchange.sendResponseHeaders(200, response.length);
    exchange.getResponseBody().write(response);
  }

  /**
   * Runs the received SQL on the native engine and serializes it as {@code /druid/v2/sql} does for {@code arrayLines}.
   * The native engine stands in for Dart, since a nested MSQ query cannot progress while the target worker waits for
   * it.
   */
  private void handleSql(final HttpExchange exchange) throws IOException
  {
    final JsonNode request = objectMapper.readTree(exchange.getRequestBody());
    sqlRequests.add(request);
    final Map<String, Object> context = new HashMap<>();
    context.put("sqlTimeZone", sourceDefaultTimeZone);
    context.putAll(objectMapper.convertValue(request.path("context"), new TypeReference<Map<String, Object>>() {}));
    context.remove("engine");
    final List<TypedValue> parameters = new ArrayList<>();
    for (final JsonNode parameter : request.path("parameters")) {
      parameters.add(TypedValue.ofLocal(ColumnMetaData.Rep.LONG, parameter.path("value").asLong()));
    }

    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    final SqlStatementFactory sourceSqlFactory =
        queryFramework().plannerFixture(PLANNER_CONFIG_DEFAULT, new AuthConfig()).statementFactory();
    try (final DirectStatement statement = sourceSqlFactory.directStatement(
        SqlQueryPlus.builder()
                    .sql(request.path("query").asText())
                    .queryContext(context)
                    .auth(CalciteTests.SUPER_USER_AUTH_RESULT)
                    .build()
                    .withParameters(parameters)
    )) {
      final DirectStatement.ResultSet resultSet = statement.plan();
      final SqlRowTransformer transformer = resultSet.createRowTransformer();
      final List<Object[]> rows = resultSet.run().getResults().toList();
      body.write(objectMapper.writeValueAsBytes(transformer.getFieldList()));
      body.write('\n');
      for (final Object[] row : rows) {
        final Object[] values = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
          values[i] = transformer.transform(row, i);
        }
        body.write(objectMapper.writeValueAsBytes(values));
        body.write('\n');
      }
      body.write('\n');
    }
    catch (Exception e) {
      final byte[] error = objectMapper.writeValueAsBytes(
          Map.of("error", "Plan validation failed", "errorMessage", String.valueOf(e.getMessage()))
      );
      exchange.sendResponseHeaders(400, error.length);
      exchange.getResponseBody().write(error);
      return;
    }
    exchange.sendResponseHeaders(200, 0);
    try (final OutputStream out = exchange.getResponseBody()) {
      body.writeTo(out);
    }
  }

  private String remote(final String extraArguments)
  {
    return StringUtils.format(
        """
        TABLE(REMOTE(endpoint => 'http://localhost:%d', dataSource => 'foo'%s))
          EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT, dim3 VARCHAR)
        """,
        server.getAddress().getPort(),
        extraArguments
    );
  }

  private String sourceSql(final int request)
  {
    return StringUtils.toUpperCase(sqlRequests.get(request).path("query").asText());
  }

  @Test
  public void testFiltersOnSourceReadsTimeRangesAndClustersOnTarget()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT __time, dim1, cnt, dim3 FROM %s
            WHERE dim1 <> ''
            PARTITIONED BY DAY
            CLUSTERED BY dim1
            """,
            remote(", splitDuration => 'P1Y'")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("dim1", ColumnType.STRING)
                        .add("cnt", ColumnType.LONG)
                        .add("dim3", ColumnType.STRING)
                        .build()
        )
        // Multi-value strings are copied as multi-value strings.
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", 1L, List.of("b", "c")},
            new Object[]{DateTimes.of("2000-01-03").getMillis(), "2", 1L, "d"},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", 1L, ""},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", 1L, null},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L, null}
        ))
        .verifyResults();

    // The source data spans two years, so it is read as two independent time ranges.
    Assertions.assertEquals(2, sqlRequests.size());
    for (int i = 0; i < sqlRequests.size(); i++) {
      Assertions.assertEquals(2, sqlRequests.get(i).path("parameters").size());
      // Clustering happens on the target.
      Assertions.assertFalse(sourceSql(i).contains("ORDER BY"), sourceSql(i));
    }
    Assertions.assertTrue(nativeRequests.stream().anyMatch(r -> "timeBoundary".equals(r.path("queryType").asText())));
  }

  @Test
  public void testRenamedColumns()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            "INSERT INTO foo1 SELECT __time, dim1 AS d, cnt AS c, dim3 AS m FROM %s"
            + " WHERE dim1 = 'abc' PARTITIONED BY DAY",
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("d", ColumnType.STRING)
                        .add("c", ColumnType.LONG)
                        .add("m", ColumnType.STRING)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L, null}))
        .verifyResults();
  }

  @Test
  public void testFilterOnRenamedGroupKey()
  {
    // The filter becomes HAVING on the source, where the alias dim1 of the sum must not shadow the column dim1.
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT * FROM (
              SELECT TIME_FLOOR(__time, 'P1D') AS __time, dim1 AS x, SUM(cnt) AS dim1 FROM %s GROUP BY 1, dim1
            )
            WHERE x = 'abc'
            PARTITIONED BY DAY
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("x", ColumnType.STRING)
                        .add("dim1", ColumnType.LONG)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}))
        .verifyResults();
  }

  @Test
  public void testEqualityFilterAboveStringExpression()
  {
    // The filter is pushed down below the expression, which runs on the target. The source would reject the read if
    // the filtered column were replaced by its value inside MV_TO_ARRAY.
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT * FROM (SELECT __time, dim1, UPPER(dim1) AS u FROM %s)
            WHERE dim1 = 'abc'
            PARTITIONED BY DAY
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("dim1", ColumnType.STRING)
                        .add("u", ColumnType.STRING)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", "ABC"}))
        .verifyResults();
  }

  @Test
  public void testFilterOnSwappedNamesOfNestedAggregation()
  {
    // Each of the six source rows has cnt 1, so the inner aggregation is (k = 1, v = 6) and the outer one swaps the
    // names to (k = 6, v = 1). The filter becomes HAVING on a subquery, whose columns must not resolve to the aliases.
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT TIMESTAMP '2000-01-01' AS __time, k, v, n
            FROM (SELECT v AS k, k AS v, COUNT(*) AS n
                  FROM (SELECT cnt AS k, COUNT(*) AS v FROM %s GROUP BY cnt)
                  GROUP BY k, v)
            WHERE k = 6
            PARTITIONED BY ALL
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("k", ColumnType.LONG)
                        .add("v", ColumnType.LONG)
                        .add("n", ColumnType.LONG)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2000-01-01").getMillis(), 6L, 1L, 1L}))
        .verifyResults();
  }

  @Test
  public void testEqualityFilterKeepsMultiValueRows()
  {
    // Rows match if any value equals 'b', and are copied with all of their values.
    testIngestQuery()
        .setSql(StringUtils.format(
            "INSERT INTO foo1 SELECT __time, dim1, dim3 FROM %s WHERE dim3 = 'b' PARTITIONED BY DAY",
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("dim1", ColumnType.STRING)
                        .add("dim3", ColumnType.STRING)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-01").getMillis(), "", List.of("a", "b")},
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", List.of("b", "c")}
        ))
        .verifyResults();
  }

  @Test
  public void testConstantGroupingKey()
  {
    // Druid reads a numeric constant in GROUP BY as a position in the select list, so the source must not see one;
    // position 4 here is COUNT(*).
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT TIMESTAMP '2000-01-01' AS __time, dim1, n, COUNT(*) AS c
            FROM (SELECT dim1, CAST(4 AS INTEGER) AS n FROM %s WHERE dim1 = 'abc')
            GROUP BY dim1, n
            PARTITIONED BY ALL
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("dim1", ColumnType.STRING)
                        .add("n", ColumnType.LONG)
                        .add("c", ColumnType.LONG)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2000-01-01").getMillis(), "abc", 4L, 1L}))
        .verifyResults();
  }

  @Test
  public void testForwardsTargetTimeZone()
  {
    sourceDefaultTimeZone = "America/Los_Angeles";
    final Map<String, Object> context = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    context.put("sqlTimeZone", "Asia/Shanghai");
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT TIME_FLOOR(__time, 'P1D') AS __time, COUNT(*) AS cnt FROM %s
            WHERE __time >= TIMESTAMP '2001-01-01 00:00:00'
            GROUP BY 1
            PARTITIONED BY DAY
            """,
            remote("")
        ))
        .setQueryContext(context)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder().add("__time", ColumnType.LONG).add("cnt", ColumnType.LONG).build()
        )
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-12-31T16:00:00Z").getMillis(), 1L},
            new Object[]{DateTimes.of("2001-01-01T16:00:00Z").getMillis(), 1L},
            new Object[]{DateTimes.of("2001-01-02T16:00:00Z").getMillis(), 1L}
        ))
        .verifyResults();

    Assertions.assertEquals("Asia/Shanghai", sqlRequests.get(0).path("context").path("sqlTimeZone").asText());
    Assertions.assertTrue(sqlRequests.get(0).path("context").path("sqlCurrentTimestamp").isTextual());
  }

  @Test
  public void testDateAndTimestampOutputsInTargetTimeZone()
  {
    // The source returns DATE and TIMESTAMP outputs as epoch milliseconds, evaluated in the target's time zone.
    sourceDefaultTimeZone = "America/Los_Angeles";
    final Map<String, Object> context = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    context.put("sqlTimeZone", "Asia/Shanghai");
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT
              CAST(CAST(__time AS DATE) AS TIMESTAMP) AS __time,
              CAST(__time AS DATE) AS "day",
              MAX(__time) AS latest,
              COUNT(*) AS cnt
            FROM %s
            WHERE dim1 = 'abc'
            GROUP BY 1, 2
            PARTITIONED BY DAY
            """,
            remote("")
        ))
        .setQueryContext(context)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(
            RowSignature.builder()
                        .add("__time", ColumnType.LONG)
                        .add("day", ColumnType.LONG)
                        .add("latest", ColumnType.LONG)
                        .add("cnt", ColumnType.LONG)
                        .build()
        )
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{
                DateTimes.of("2001-01-02T16:00:00Z").getMillis(),
                DateTimes.of("2001-01-02T16:00:00Z").getMillis(),
                DateTimes.of("2001-01-03T00:00:00Z").getMillis(),
                1L
            }
        ))
        .verifyResults();
    // The DATE output is cast to TIMESTAMP on the source to be returned as milliseconds.
    Assertions.assertTrue(
        sourceSql(0).contains("TIMESTAMP_TO_MILLIS(CAST(CAST(\"__TIME\" AS DATE) AS TIMESTAMP(3))) AS \"DAY\""),
        sourceSql(0)
    );
    Assertions.assertTrue(sourceSql(0).contains("TIMESTAMP_TO_MILLIS(MAX("), sourceSql(0));
  }

  @Test
  public void testBoundPasswordAndDiscoveredSchema()
  {
    expectedAuthorization = "Basic cmVhZGVyOnNlY3JldA==";
    final String remote = StringUtils.format(
        """
        TABLE(REMOTE(endpoint => 'http://localhost:%d/druid/v2/', dataSource => 'foo',
                    authType => 'basic', userName => 'reader', password => ?))
        """,
        server.getAddress().getPort()
    );
    testIngestQuery()
        .setSql(StringUtils.format("INSERT INTO foo1 SELECT * FROM %s WHERE dim1 = 'abc' PARTITIONED BY DAY", remote))
        .setDynamicParameters(List.of(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "secret")))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("dim1", ColumnType.STRING).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc"}))
        .verifyResults();
    // Validation and conversion share one discovery.
    Assertions.assertEquals(
        1,
        nativeRequests.stream().filter(r -> "segmentMetadata".equals(r.path("queryType").asText())).count()
    );
    Assertions.assertEquals(1, sqlRequests.size());
    // A discovered schema has the source's own types, so no casts are needed.
    Assertions.assertFalse(sourceSql(0).contains("CAST(\"CNT\""), sourceSql(0));
  }

}
