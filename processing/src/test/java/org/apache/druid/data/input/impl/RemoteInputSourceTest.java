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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class RemoteInputSourceTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("__time", ColumnType.LONG)
                                                            .add("a", ColumnType.STRING)
                                                            .add("flag", ColumnType.LONG)
                                                            .add("arr", ColumnType.LONG_ARRAY)
                                                            .build();
  private static final String HEADER = "[\"__time\",\"a\",\"flag\",\"arr\"]\n";

  private static final long DEFAULT_RETRY_BACKOFF_MILLIS = RemoteInputSource.retryBackoffMillis;

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<JsonNode> sqlRequests = new CopyOnWriteArrayList<>();
  // Recorded by the source and checked on the test thread, since a failed assertion in a handler only breaks the
  // connection.
  private final List<JsonNode> nativeRequests = new CopyOnWriteArrayList<>();
  private final List<String> canceledQueries = new CopyOnWriteArrayList<>();
  private final Queue<Response> sqlResponses = new ConcurrentLinkedQueue<>();
  private final List<String> acceptEncodings = new CopyOnWriteArrayList<>();
  private volatile boolean gzipResponses;
  private volatile int timeBoundaryStatus = 200;
  private volatile String timeBoundaryResponse =
      """
      [{"timestamp":"2020-01-01T00:00:00.000Z",
        "result":{"minTime":"2020-01-01T00:00:00.000Z","maxTime":"2020-01-03T05:00:00.000Z"}}]
      """;
  private HttpServer server;
  private RemoteConnection connection;

  private record Response(int status, String body)
  {
  }

  @BeforeEach
  public void setUp() throws IOException
  {
    RemoteInputSource.retryBackoffMillis = 0;
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
          acceptEncodings.add(String.valueOf(exchange.getRequestHeaders().getFirst("Accept-Encoding")));
          response = sqlResponses.isEmpty() ? new Response(500, "{\"error\":\"no response\"}") : sqlResponses.poll();
        } else {
          nativeRequests.add(request);
          response = new Response(timeBoundaryStatus, timeBoundaryResponse);
        }
        if (gzipResponses) {
          exchange.getResponseHeaders().set("Content-Encoding", "gzip");
        }
        exchange.sendResponseHeaders(response.status, 0);
        try (final OutputStream out = gzipResponses
                                      ? new GZIPOutputStream(exchange.getResponseBody())
                                      : exchange.getResponseBody()) {
          out.write(StringUtils.toUtf8(response.body));
        }
      }
    });
    server.start();
    connection = new RemoteConnection(
        URI.create("http://localhost:" + server.getAddress().getPort()),
        null,
        1000,
        5000,
        2,
        null
    );
    mapper.setInjectableValues(new InjectableValues.Std()
                                   .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                   .addValue(ObjectMapper.class, mapper)
                                   .addValue(RemoteTlsContext.class, new RemoteTlsContext()));
  }

  @AfterEach
  public void tearDown()
  {
    RemoteInputSource.retryBackoffMillis = DEFAULT_RETRY_BACKOFF_MILLIS;
    server.stop(0);
    for (final JsonNode request : nativeRequests) {
      Assertions.assertEquals("timeBoundary", request.path("queryType").asText());
    }
  }

  private RemoteInputSource source(
      final boolean timeRangeParameters,
      final List<Interval> intervals,
      final Period splitDuration
  )
  {
    return RemoteInputSource.table(
        connection,
        "events",
        intervals,
        null,
        splitDuration,
        null,
        true,
        new HttpInputSourceConfig(null, null),
        mapper
    ).withPlannedQuery(
        "SELECT \"__time\", \"a\", \"flag\", \"arr\" FROM \"events\"",
        timeRangeParameters,
        SIGNATURE,
        List.of("__time"),
        Map.of("sqlTimeZone", "UTC"),
        null
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
    try (final CloseableIterator<InputRow> iterator = source.reader(schema, null, null).read()) {
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
  public void testReadsGzippedResults() throws IOException
  {
    gzipResponses = true;
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[1]]\n\n"));
    final List<InputRow> rows = read(source(false, null, null));
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("x", rows.get(0).getRaw("a"));
    Assertions.assertEquals(List.of("gzip"), acceptEncodings);
  }

  @Test
  public void testUnwrapsMultiValueStrings() throws IOException
  {
    sqlResponses.add(new Response(
        200,
        HEADER + """
            ["2020-01-01T00:00:00.000Z",["x","y"],1,null]
            ["2020-01-01T00:00:00.000Z",["z"],1,null]
            ["2020-01-01T00:00:00.000Z",[],1,[1]]

            """
    ));
    final List<InputRow> rows = read(source(false, null, null));
    Assertions.assertEquals(List.of("x", "y"), rows.get(0).getRaw("a"));
    Assertions.assertEquals("z", rows.get(1).getRaw("a"));
    Assertions.assertNull(rows.get(2).getRaw("a"));
    // Array columns keep their arrays, even with one element.
    Assertions.assertEquals(List.of(1), rows.get(2).getRaw("arr"));
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
    // Ranges are aligned to period boundaries, even when the data starts mid-period.
    Assertions.assertEquals(
        List.of(
            Intervals.of("2020-01-01T03:17:00.000Z/2020-01-02T00:00:00.000Z"),
            Intervals.of("2020-01-02/2020-01-03"),
            Intervals.of("2020-01-03T00:00:00.000Z/2020-01-03T05:00:00.001Z")
        ),
        splits(source(true, List.of(Intervals.of("2020-01-01T03:17/2021")), Period.days(1)))
    );
    // Without bounds, one range is still read so the source rejects a missing datasource.
    timeBoundaryResponse = "[]";
    Assertions.assertEquals(List.of(Intervals.ETERNITY), splits(source(true, null, Period.days(1))));
    Assertions.assertEquals(
        List.of(Intervals.of("2021/2022"), Intervals.of("2023/2024")),
        splits(source(true, List.of(Intervals.of("2023/2024"), Intervals.of("2021/2022")), Period.days(1)))
    );
    // Empty intervals request nothing.
    Assertions.assertTrue(splits(source(true, List.of(Intervals.of("2021/2021")), Period.days(1))).isEmpty());
    // Aggregating source SQL is read in one piece without time parameters.
    Assertions.assertEquals(List.of(Intervals.ETERNITY), splits(source(false, null, null)));
  }

  @Test
  public void testRejectedTimeBoundaryIsInvalidInput()
  {
    timeBoundaryStatus = 401;
    timeBoundaryResponse = "{\"error\":\"Unauthorized\"}";
    final DruidException e =
        Assertions.assertThrows(DruidException.class, () -> splits(source(true, null, Period.days(1))));
    Assertions.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assertions.assertTrue(e.getMessage().contains("rejected the credentials"), e.getMessage());
    Assertions.assertEquals(1, nativeRequests.size());
  }

  @Test
  public void testBoundSplitSendsTimeRangeParameters() throws IOException
  {
    final RemoteInputSource unbound = source(true, null, null);
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
  public void testCancelsInterruptedRead() throws IOException
  {
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[]]\n"));
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("__time", "auto", DateTimes.utc(0)),
        DimensionsSpec.builder().setDimensions(DimensionsSpec.getDefaultSchemas(List.of("a"))).build(),
        ColumnsFilter.all()
    );
    try (final CloseableIterator<InputRow> iterator = source(false, null, null).reader(schema, null, null).read()) {
      iterator.next();
      Thread.currentThread().interrupt();
    }
    finally {
      // The interrupt survives the cancel, which is still sent.
      Assertions.assertTrue(Thread.interrupted());
    }
    Assertions.assertEquals(1, canceledQueries.size());
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
  public void testStopsRetryingAfterMaxRetries()
  {
    // The connection allows two retries, so the third failed attempt is the last one.
    for (int i = 0; i < 3; i++) {
      sqlResponses.add(new Response(503, "{\"error\":\"busy\"}"));
    }
    sqlResponses.add(new Response(200, HEADER + "\n"));
    final UncheckedIOException e = readFailure(UncheckedIOException.class);
    Assertions.assertTrue(e.getMessage().contains("HTTP status[503]"), e.getMessage());
    Assertions.assertEquals(3, sqlRequests.size());
  }

  @Test
  public void testRejectsCorruptStreamsWithoutRetry()
  {
    final String row = "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[]]\n";
    final Map<String, String> corrupt = new LinkedHashMap<>();
    corrupt.put("<html>proxy error</html>\n", "Unexpected character");
    corrupt.put("[\"__time\",\"a\"]\n\n", "columns [__time, a], but [__time, a, flag, arr] were planned");
    // Rows are mapped by position, so the same columns in another order are rejected too.
    corrupt.put("[\"__time\",\"flag\",\"a\",\"arr\"]\n\n", "columns [__time, flag, a, arr], but");
    corrupt.put("null\n\n", "columns null, but");
    corrupt.put(HEADER + "null\n\n", "unexpected number of columns");
    corrupt.put(HEADER + row + "\n" + row, "data after its completion marker");
    corrupt.put(HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\"]\n\n", "unexpected number of columns");
    corrupt.put(HEADER + "[\"not a time\",\"x\",true,[]]\n\n", "invalid timestamp in column[__time]");
    for (final Map.Entry<String, String> response : corrupt.entrySet()) {
      sqlRequests.clear();
      sqlResponses.clear();
      sqlResponses.add(new Response(200, response.getKey()));
      // Would succeed if the read were retried.
      sqlResponses.add(new Response(200, HEADER + "\n"));
      // A corrupt result is not retried here, nor by relaunching the reader.
      final DruidException e = readFailure(DruidException.class);
      Assertions.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory(), response.getKey());
      Assertions.assertTrue(e.getMessage().contains(response.getValue()), response.getKey() + " -> " + e.getMessage());
      Assertions.assertEquals(1, sqlRequests.size(), response.getKey());
    }
  }

  @Test
  public void testReadsResultsLargerThanTheBackpressureLimit() throws IOException
  {
    // About 1 MB, so the transport suspends and resumes reading many times.
    final StringBuilder body = new StringBuilder(HEADER);
    for (int i = 0; i < 20_000; i++) {
      body.append("[\"2020-01-01T00:00:00.000Z\",\"row-").append(i).append("-padding-padding\",true,[]]\n");
    }
    sqlResponses.add(new Response(200, body.append('\n').toString()));
    final List<InputRow> rows = read(source(false, null, null));
    Assertions.assertEquals(20_000, rows.size());
    Assertions.assertEquals("row-19999-padding-padding", rows.get(19_999).getRaw("a"));
  }

  @Test
  public void testDoesNotRetryUserErrors()
  {
    sqlResponses.add(new Response(
        400,
        "{\"error\":\"Plan validation failed\",\"errorMessage\":\"Column 'x' not found\"}"
    ));
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> read(source(false, null, null)));
    Assertions.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assertions.assertTrue(e.getMessage().contains("Column 'x' not found"), e.getMessage());
    Assertions.assertEquals(1, sqlRequests.size());
    // Druid returns 501 for unsupported queries, which retrying cannot fix either.
    sqlRequests.clear();
    sqlResponses.add(new Response(501, "{\"error\":\"Unsupported operation\"}"));
    Assertions.assertEquals(
        DruidException.Category.INVALID_INPUT,
        Assertions.assertThrows(DruidException.class, () -> read(source(false, null, null))).getCategory()
    );
    Assertions.assertEquals(1, sqlRequests.size());
  }

  @Test
  public void testFailsWithoutRetryAfterRowsWereReturned()
  {
    // The source stopped streaming before its completion marker, after a row was already handed out.
    sqlResponses.add(new Response(200, HEADER + "[\"2020-01-01T00:00:00.000Z\",\"x\",true,[]]\n"));
    sqlResponses.add(new Response(200, HEADER + "\n"));
    // A relaunched reader rereads the whole range, so this failure stays retryable for MSQ.
    final UncheckedIOException e = readFailure(UncheckedIOException.class);
    Assertions.assertTrue(e.getMessage().contains("completion marker"), e.getMessage());
    Assertions.assertEquals(1, sqlRequests.size());
    Assertions.assertEquals(1, canceledQueries.size());
  }

  @Test
  public void testSerde() throws IOException
  {
    final RemoteInputSource source = source(true, List.of(Intervals.of("2020/2021")), Period.days(1));
    final InputSource roundTrip = mapper.readValue(mapper.writeValueAsString(source), InputSource.class);
    Assertions.assertEquals(source, roundTrip);
    Assertions.assertEquals(Set.of(RemoteInputSource.TYPE_KEY), roundTrip.getTypes());
  }

  @Test
  public void testRejectsSplitDurationWithoutTimeRangeParameters()
  {
    final ObjectNode json = mapper.valueToTree(source(true, null, Period.days(1)));
    json.remove("timeRangeParameters");
    final Exception e = Assertions.assertThrows(Exception.class, () -> mapper.treeToValue(json, InputSource.class));
    Assertions.assertTrue(e.getMessage().contains("splitDuration applies only"), e.getMessage());
  }

  /**
   * Reads the plain source and returns the failure, which the test expects.
   */
  private <T extends Throwable> T readFailure(final Class<T> type)
  {
    return Assertions.assertThrows(type, () -> read(source(false, null, null)));
  }

  private static List<Interval> splits(final RemoteInputSource source) throws IOException
  {
    return source.createSplits(null, null).map(InputSplit::get).collect(Collectors.toList());
  }
}
