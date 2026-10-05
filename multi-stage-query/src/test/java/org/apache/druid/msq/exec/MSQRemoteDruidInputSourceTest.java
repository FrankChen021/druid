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

/// Runs target MSQ ingestion against a loopback source whose SQL API executes the pushed-down SQL with a real Druid
/// SQL planner and engine, then streams it back the way `/druid/v2/sql` does for `resultFormat: arrayLines`.
public class MSQRemoteDruidInputSourceTest extends MSQTestBase
{
  private static final RowSignature FOO_SIGNATURE = RowSignature.builder()
                                                                .add("__time", ColumnType.LONG)
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("cnt", ColumnType.LONG)
                                                                .build();

  private final List<JsonNode> nativeRequests = new CopyOnWriteArrayList<>();
  private final List<JsonNode> sqlRequests = new CopyOnWriteArrayList<>();
  private volatile String sourceDefaultTimeZone = "UTC";
  private volatile String expectedAuthorization;
  private HttpServer server;

  @BeforeEach
  public void startSource() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        Assertions.assertEquals(expectedAuthorization, exchange.getRequestHeaders().getFirst("Authorization"));
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

  /// Runs the received SQL on the native engine and serializes it as `/druid/v2/sql` does for `arrayLines`. The
  /// native engine stands in for Dart, since a nested MSQ query cannot progress while the target worker waits for it.
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
      Assertions.assertEquals("BIGINT", parameter.path("type").asText());
      parameters.add(TypedValue.ofLocal(ColumnMetaData.Rep.LONG, parameter.path("value").asLong()));
    }

    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    final SqlStatementFactory sourceSqlFactory =
        queryFramework().plannerFixture(PLANNER_CONFIG_DEFAULT, new AuthConfig()).statementFactory();
    try (DirectStatement statement = sourceSqlFactory.directStatement(
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
    try (OutputStream out = exchange.getResponseBody()) {
      body.writeTo(out);
    }
  }

  private String remote(final String extraArguments)
  {
    return StringUtils.format(
        """
        TABLE(DRUID(endpoint => 'http://localhost:%d', dataSource => 'foo'%s))
          EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)
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
  public void testFiltersOnSourceAndReadsTimeRangesInParallel()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT __time, dim1, cnt FROM %s
            WHERE dim1 <> ''
            PARTITIONED BY DAY
            """,
            remote(", splitDuration => 'P1Y'")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(FOO_SIGNATURE)
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", 1L},
            new Object[]{DateTimes.of("2000-01-03").getMillis(), "2", 1L},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", 1L},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", 1L},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}
        ))
        .verifyResults();

    // The source data spans two years, so it is read as two independent time ranges.
    Assertions.assertEquals(2, sqlRequests.size());
    for (int i = 0; i < sqlRequests.size(); i++) {
      Assertions.assertTrue(sourceSql(i).contains("\"DIM1\" <> ''"), sourceSql(i));
      Assertions.assertEquals(2, sqlRequests.get(i).path("parameters").size());
      Assertions.assertEquals("msq-dart", sqlRequests.get(i).path("context").path("engine").asText());
    }
    Assertions.assertTrue(nativeRequests.stream().anyMatch(r -> "timeBoundary".equals(r.path("queryType").asText())));
  }

  @Test
  public void testAggregatesOnSourceInOneQuery()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, cnt, SUM(cnt) AS sum_cnt FROM %s
            WHERE dim1 IN ('2', 'abc')
            GROUP BY cnt
            HAVING SUM(cnt) > 1
            PARTITIONED BY DAY
            """,
            remote(", splitDuration => 'P1D'")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, 2L}))
        .verifyResults();

    // Aggregation results cannot be split by time, so splitDuration does not apply.
    Assertions.assertEquals(1, sqlRequests.size());
    Assertions.assertTrue(sqlRequests.get(0).path("parameters").isMissingNode());
    Assertions.assertTrue(sourceSql(0).contains("GROUP BY"), sourceSql(0));
    Assertions.assertTrue(sourceSql(0).contains("HAVING"), sourceSql(0));
  }

  @Test
  public void testAggregatesWithinRequestedIntervals()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, cnt, SUM(cnt) AS sum_cnt FROM %s
            GROUP BY cnt
            PARTITIONED BY DAY
            """,
            remote(", intervals => ARRAY['2000-01-02/2000-01-03', '2001-01-01/2001-01-02']")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, 2L}))
        .verifyResults();

    Assertions.assertEquals(1, sqlRequests.size());
    Assertions.assertTrue(sourceSql(0).contains("MILLIS_TO_TIMESTAMP(946771200000)"), sourceSql(0));
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
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG).add("cnt", ColumnType.LONG).build())
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
  public void testKeepsClusterByOnTheTarget()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT __time, dim1, cnt FROM %s
            WHERE dim1 <> ''
            PARTITIONED BY DAY
            CLUSTERED BY dim1
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(FOO_SIGNATURE)
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", 1L},
            new Object[]{DateTimes.of("2000-01-03").getMillis(), "2", 1L},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", 1L},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", 1L},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}
        ))
        .verifyResults();

    Assertions.assertFalse(sourceSql(0).contains("ORDER BY"), sourceSql(0));
  }

  @Test
  public void testEmptySourceResult()
  {
    testIngestQuery()
        .setSql(StringUtils.format("INSERT INTO foo1 SELECT * FROM %s WHERE dim1 = 'no-such-row' PARTITIONED BY DAY", remote("")))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(FOO_SIGNATURE)
        .setExpectedResultRows(List.of())
        .verifyResults();
    Assertions.assertEquals(1, sqlRequests.size());
  }

  @Test
  public void testSourceErrorFailsIngestion()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT __time, no_such_column
            FROM TABLE(DRUID(endpoint => 'http://localhost:%d', dataSource => 'foo'))
              EXTEND (__time BIGINT, no_such_column VARCHAR)
            PARTITIONED BY DAY
            """,
            server.getAddress().getPort()
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedExecutionErrorMatcher(e -> {
          Throwable cause = e;
          boolean found = false;
          while (cause != null) {
            found |= String.valueOf(cause.getMessage()).contains("HTTP status[400]");
            cause = cause.getCause();
          }
          Assertions.assertTrue(found, String.valueOf(e));
        })
        .verifyExecutionError();
    Assertions.assertEquals(1, sqlRequests.size());
  }

  @Test
  public void testBasicAuthenticationAndDiscoveredSchemaWithBoundPassword()
  {
    expectedAuthorization = "Basic cmVhZGVyOnNlY3JldA==";
    final String remote = StringUtils.format(
        """
        TABLE(DRUID(endpoint => 'http://localhost:%d/druid/v2/', dataSource => 'foo',
                    authType => 'basic', username => 'reader', password => ?))
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
    Assertions.assertTrue(nativeRequests.stream().anyMatch(r -> "segmentMetadata".equals(r.path("queryType").asText())));
    Assertions.assertEquals(1, sqlRequests.size());
    Assertions.assertFalse(sourceSql(0).contains(StringUtils.toUpperCase("secret")));
  }

  @Test
  public void testJoinWithLocalTableStaysOnTarget()
  {
    testIngestQuery()
        .setSql(StringUtils.format(
            """
            INSERT INTO foo1
            SELECT r.__time, r.dim1, f.dim2
            FROM %s r INNER JOIN foo f ON r.dim1 = f.dim1
            WHERE r.dim1 IN ('1', 'def')
            PARTITIONED BY DAY
            """,
            remote("")
        ))
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("dim2", ColumnType.STRING).build())
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", "a"},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", "abc"}
        ))
        .verifyResults();
    Assertions.assertTrue(sourceSql(0).contains("IN ('1', 'DEF')"), sourceSql(0));
    Assertions.assertFalse(sourceSql(0).contains("JOIN"), sourceSql(0));
  }
}
