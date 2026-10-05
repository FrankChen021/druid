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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class RemoteDruidSqlInputSourceTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("__time", ColumnType.LONG)
                                                            .add("a", ColumnType.STRING)
                                                            .add("flag", ColumnType.LONG)
                                                            .add("arr", ColumnType.LONG_ARRAY)
                                                            .build();
  private static final String HEADER = "[\"__time\",\"a\",\"flag\",\"arr\"]\n";

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<JsonNode> sqlRequests = new CopyOnWriteArrayList<>();
  private final List<String> canceledQueries = new CopyOnWriteArrayList<>();
  private final Queue<Response> sqlResponses = new ConcurrentLinkedQueue<>();
  private volatile String timeBoundaryResponse =
      """
      [{"timestamp":"2020-01-01T00:00:00.000Z",
        "result":{"minTime":"2020-01-01T00:00:00.000Z","maxTime":"2020-01-03T05:00:00.000Z"}}]""";
  private HttpServer server;
  private RemoteDruidConnection connection;

  private record Response(int status, String body)
  {
  }

  @BeforeEach
  public void setUp() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        final String path = exchange.getRequestURI().getPath();
        if ("DELETE".equals(exchange.getRequestMethod())) {
          canceledQueries.add(path.substring(path.lastIndexOf('/') + 1));
          exchange.sendResponseHeaders(202, -1);
          return;
        }
        final JsonNode request = mapper.readTree(exchange.getRequestBody());
        final Response response;
        if ("/druid/v2/sql".equals(path)) {
          sqlRequests.add(request);
          response = sqlResponses.isEmpty() ? new Response(500, "{\"error\":\"no response\"}") : sqlResponses.poll();
        } else {
          Assertions.assertEquals("timeBoundary", request.path("queryType").asText());
          response = new Response(200, timeBoundaryResponse);
        }
        exchange.sendResponseHeaders(response.status, 0);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(StringUtils.toUtf8(response.body));
        }
      }
    });
    server.start();
    connection = new RemoteDruidConnection(
        URI.create("http://localhost:" + server.getAddress().getPort()),
        null,
        1000,
        5000,
        2
    );
    mapper.setInjectableValues(new InjectableValues.Std()
                                   .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                   .addValue(ObjectMapper.class, mapper));
  }

  @AfterEach
  public void tearDown()
  {
    server.stop(0);
  }

  private RemoteDruidSqlInputSource source(
      final boolean timeRangeParameters,
      final List<Interval> intervals,
      final Period splitDuration
  )
  {
    return new RemoteDruidSqlInputSource(
        connection,
        "events",
        "SELECT \"__time\", \"a\", \"flag\", \"arr\" FROM \"events\"",
        null,
        Map.of("sqlTimeZone", "UTC"),
        SIGNATURE,
        List.of("__time"),
        timeRangeParameters,
        intervals,
        splitDuration,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    );
  }

  private static List<InputRow> read(final InputSource source) throws IOException
  {
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("__time", "auto", DateTimes.utc(0)),
        DimensionsSpec.builder().setDimensions(DimensionsSpec.getDefaultSchemas(List.of("a", "flag", "arr"))).build(),
        ColumnsFilter.all()
    );
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = source.reader(schema, null, null).read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  @Test
  public void testReadsStreamedRowsAndConvertsValues() throws IOException
  {
    sqlResponses.add(new Response(
        200,
        HEADER + """
            ["2020-01-01T08:00:00.000+08:00","x",true,[1,2]]
            ["2020-01-02T00:00:00.000Z",null,false,null]

            """
    ));
    final List<InputRow> rows = read(source(false, null, null));

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(DateTimes.of("2020-01-01").getMillis(), rows.get(0).getTimestampFromEpoch());
    Assertions.assertEquals("x", rows.get(0).getRaw("a"));
    Assertions.assertEquals(1L, rows.get(0).getRaw("flag"));
    Assertions.assertEquals(List.of(1, 2), rows.get(0).getRaw("arr"));
    Assertions.assertEquals(0L, rows.get(1).getRaw("flag"));
    Assertions.assertNull(rows.get(1).getRaw("a"));

    final JsonNode request = sqlRequests.get(0);
    Assertions.assertEquals("arrayLines", request.path("resultFormat").asText());
    Assertions.assertTrue(request.path("header").asBoolean());
    Assertions.assertTrue(request.path("parameters").isMissingNode());
    Assertions.assertEquals("msq-dart", request.path("context").path("engine").asText());
    Assertions.assertFalse(request.path("context").path("sqlStringifyArrays").asBoolean());
    Assertions.assertEquals("UTC", request.path("context").path("sqlTimeZone").asText());
    Assertions.assertTrue(canceledQueries.isEmpty());
  }

  @Test
  public void testEmptyResult() throws IOException
  {
    sqlResponses.add(new Response(200, HEADER + "\n"));
    Assertions.assertTrue(read(source(false, null, null)).isEmpty());
  }

  @Test
  public void testTimeRangeSplits() throws IOException
  {
    final List<Interval> daily = splits(source(true, null, Period.days(1)));
    Assertions.assertEquals(
        List.of(
            Intervals.of("2020-01-01/2020-01-02"),
            Intervals.of("2020-01-02/2020-01-03"),
            Intervals.of("2020-01-03T00:00:00.000Z/2020-01-03T05:00:00.001Z")
        ),
        daily
    );
    Assertions.assertEquals(
        List.of(Intervals.of("2020-01-02T12:00:00.000Z/2020-01-03T05:00:00.001Z")),
        splits(source(true, List.of(Intervals.of("2020-01-02T12/2021")), null))
    );
    Assertions.assertTrue(splits(source(true, List.of(Intervals.of("2021/2022")), null)).isEmpty());
    timeBoundaryResponse = "[]";
    Assertions.assertTrue(splits(source(true, null, Period.days(1))).isEmpty());
    // Aggregating source SQL is read in one piece without time parameters.
    Assertions.assertEquals(List.of(Intervals.ETERNITY), splits(source(false, null, null)));
  }

  @Test
  public void testBoundSplitSendsTimeRangeParameters() throws IOException
  {
    final RemoteDruidSqlInputSource unbound = source(true, null, null);
    Assertions.assertThrows(IllegalStateException.class, () -> read(unbound));
    final InputSource bound = unbound.withSplit(new InputSplit<>(Intervals.of("2020-01-01/2020-01-02")));
    sqlResponses.add(new Response(200, HEADER + "\n"));
    read(bound);
    final JsonNode parameters = sqlRequests.get(0).path("parameters");
    Assertions.assertEquals("BIGINT", parameters.get(0).path("type").asText());
    Assertions.assertEquals(DateTimes.of("2020-01-01").getMillis(), parameters.get(0).path("value").asLong());
    Assertions.assertEquals(DateTimes.of("2020-01-02").getMillis(), parameters.get(1).path("value").asLong());
  }

  @Test
  public void testRetriesTransientFailuresBeforeFirstRow() throws IOException
  {
    sqlResponses.add(new Response(503, "{\"error\":\"busy\"}"));
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\""));
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[]]\n\n"));
    Assertions.assertEquals(1, read(source(false, null, null)).size());
    Assertions.assertEquals(3, sqlRequests.size());
    Assertions.assertEquals(1, canceledQueries.size());
  }

  @Test
  public void testDoesNotRetryUserErrors()
  {
    sqlResponses.add(new Response(400, "{\"error\":\"Plan validation failed\",\"errorMessage\":\"Column 'x' not found\"}"));
    final UncheckedIOException e = Assertions.assertThrows(UncheckedIOException.class, () -> read(source(false, null, null)));
    Assertions.assertTrue(e.getMessage().contains("Column 'x' not found"), e.getMessage());
    Assertions.assertEquals(1, sqlRequests.size());
  }

  @Test
  public void testFailsWithoutRetryAfterRowsWereReturned()
  {
    // The source stopped streaming before its completion marker, after a row was already handed out.
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[]]\n"));
    sqlResponses.add(new Response(200, HEADER + "\n"));
    final UncheckedIOException e = Assertions.assertThrows(UncheckedIOException.class, () -> read(source(false, null, null)));
    Assertions.assertTrue(e.getMessage().contains("completion marker"), e.getMessage());
    Assertions.assertEquals(1, sqlRequests.size());
    Assertions.assertEquals(1, canceledQueries.size());
  }

  @Test
  public void testRejectsUnexpectedColumns()
  {
    sqlResponses.add(new Response(200, "[\"__time\",\"a\"]\n\n"));
    final UncheckedIOException e = Assertions.assertThrows(UncheckedIOException.class, () -> read(source(false, null, null)));
    Assertions.assertTrue(e.getMessage().contains("2 columns, but 4 were planned"), e.getMessage());
    Assertions.assertEquals(1, sqlRequests.size());
  }

  @Test
  public void testMissingProviderPasswordFailsWithoutRequest()
  {
    connection = new RemoteDruidConnection(
        connection.getEndpoint(),
        new RemoteDruidAuthentication.Basic("reader", new EnvironmentVariablePasswordProvider("NO_SUCH_ENV_VAR_FOR_TEST")),
        1000,
        5000,
        0
    );
    final UncheckedIOException e = Assertions.assertThrows(UncheckedIOException.class, () -> read(source(false, null, null)));
    Assertions.assertTrue(e.getMessage().contains("did not return a password"), e.getMessage());
    Assertions.assertTrue(sqlRequests.isEmpty());
  }

  @Test
  public void testSerde() throws IOException
  {
    final RemoteDruidSqlInputSource source = source(true, List.of(Intervals.of("2020/2021")), Period.days(1));
    final InputSource roundTrip = mapper.readValue(mapper.writeValueAsString(source), InputSource.class);
    Assertions.assertEquals(source, roundTrip);
    Assertions.assertEquals(java.util.Set.of(RemoteDruidInputSource.TYPE_KEY), roundTrip.getTypes());
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidSqlInputSource(
        connection,
        "events",
        "SELECT 1",
        "unknown",
        null,
        SIGNATURE,
        null,
        false,
        null,
        null,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    ));
  }

  private static List<Interval> splits(final RemoteDruidSqlInputSource source) throws IOException
  {
    return source.createSplits(null, null).map(InputSplit::get).collect(Collectors.toList());
  }
}
