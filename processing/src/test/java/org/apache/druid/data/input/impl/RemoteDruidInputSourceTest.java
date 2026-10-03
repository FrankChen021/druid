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

import com.fasterxml.jackson.core.JsonParser;
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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class RemoteDruidInputSourceTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private final List<String> canceledQueries = new CopyOnWriteArrayList<>();
  private final AtomicInteger failures = new AtomicInteger();
  private final AtomicInteger status = new AtomicInteger(200);
  private final AtomicReference<String> scanResponse = new AtomicReference<>(
      "[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[0,\"keep\",10]]}]"
  );
  private final AtomicReference<String> boundaryResponse = new AtomicReference<>(
      "[{\"result\":{\"minTime\":\"1970-01-01T00:00:00Z\",\"maxTime\":\"1970-01-01T01:00:00Z\"}}]"
  );
  private final AtomicReference<String> metadataResponse = new AtomicReference<>(
      "[{\"columns\":{\"__time\":{\"typeSignature\":\"LONG\"},"
      + "\"a\":{\"type\":\"STRING\"},\"values\":{\"typeSignature\":\"ARRAY<LONG>\"}}}]"
  );
  private HttpServer server;
  private RemoteDruidConnection config;

  @BeforeEach
  public void setUp() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        Assertions.assertEquals("Basic dXNlcjpzZWNyZXQ=", exchange.getRequestHeaders().getFirst("Authorization"));
        if ("DELETE".equals(exchange.getRequestMethod())) {
          canceledQueries.add(exchange.getRequestURI().getPath().substring("/druid/v2/".length()));
          exchange.sendResponseHeaders(200, -1);
          return;
        }
        final JsonNode query = mapper.readTree(exchange.getRequestBody());
        requests.add(query);
        final boolean scan = "scan".equals(query.path("queryType").asText());
        final String completeResponse = scan ? scanResponse.get()
                                        : "segmentMetadata".equals(query.path("queryType").asText())
                                          ? metadataResponse.get() : boundaryResponse.get();
        // Include a complete row before EOF so retrying after early exposure would duplicate it.
        final String response = scan && failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0
                                ? completeResponse.substring(0, completeResponse.length() - 1) : completeResponse;
        final byte[] bytes = StringUtils.toUtf8(response);
        exchange.sendResponseHeaders(status.get(), bytes.length);
        exchange.getResponseBody().write(bytes);
      }
    });
    server.start();
    config = config(1024 * 1024, 1);
    mapper.setInjectableValues(new InjectableValues.Std()
                                  .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                  .addValue(ObjectMapper.class, mapper));
  }

  @AfterEach
  public void tearDown()
  {
    server.stop(0);
  }

  private RemoteDruidConnection config(final long maxBytes, final int retries)
  {
    return new RemoteDruidConnection(
        URI.create("http://localhost:" + server.getAddress().getPort()),
        new RemoteDruidAuthentication.Basic("user", new DefaultPasswordProvider("secret")),
        1000, 1000, maxBytes, retries
    );
  }

  private RemoteDruidInputSource source(final List<Interval> intervals)
  {
    return new RemoteDruidInputSource(config, "events", intervals, null, null, false,
                                      new HttpInputSourceConfig(null, null), mapper);
  }

  private InputRowSchema schema()
  {
    return new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(List.of(new StringDimensionSchema("a"), new LongDimensionSchema("bytes"))),
        ColumnsFilter.all()
    );
  }

  @Test
  public void testDiscoveryAndFixedSplits() throws IOException
  {
    final RemoteDruidInputSource source = source(null);
    final List<InputSplit<Interval>> splits;
    try (final Stream<InputSplit<Interval>> stream = source.createSplits(null, null)) {
      splits = stream.toList();
    }
    Assertions.assertEquals(List.of(Intervals.utc(0, 3_600_000), Intervals.utc(3_600_000, 3_600_001)),
                            splits.stream().map(InputSplit::get).toList());
    final RemoteDruidInputSource leaf = source.withSplit(splits.get(0));
    try (final Stream<InputSplit<Interval>> stream = leaf.createSplits(null, null)) {
      Assertions.assertEquals(List.of(splits.get(0).get()), stream.map(InputSplit::get).toList());
    }
    final String json = mapper.writeValueAsString(leaf);
    Assertions.assertTrue(json.contains("endpoint"));
    Assertions.assertFalse(leaf.toString().contains("secret"));
    final RemoteDruidInputSource restored = (RemoteDruidInputSource) mapper.readValue(json, InputSource.class);
    Assertions.assertEquals(leaf, restored);
    Assertions.assertEquals(1, restored.estimateNumSplits(null, null));
    Assertions.assertEquals(1, requests.size());
  }

  @Test
  public void testNumericTimeBoundaryTimestamps() throws IOException
  {
    boundaryResponse.set("[{\"result\":{\"minTime\":0,\"maxTime\":0}}]");
    try (final Stream<InputSplit<Interval>> splits = source(null).createSplits(null, null)) {
      Assertions.assertEquals(List.of(Intervals.utc(0, 1)), splits.map(InputSplit::get).toList());
    }
    boundaryResponse.set("[{\"result\":{\"minTime\":0.5,\"maxTime\":1}}]");
    Assertions.assertThrows(IOException.class, () -> source(null).estimateNumSplits(null, null));
  }

  @Test
  public void testExplicitIntervalsAreIntersected() throws IOException
  {
    final RemoteDruidInputSource source = source(List.of(Intervals.utc(-1000, 1000), Intervals.utc(500, 2000), Intervals.utc(3000, 5000)));
    try (final Stream<InputSplit<Interval>> stream = source.createSplits(null, null)) {
      Assertions.assertEquals(List.of(Intervals.utc(0, 2000), Intervals.utc(3000, 5000)), stream.map(InputSplit::get).toList());
    }
    final RemoteDruidInputSource filtered = source.withReadFilter(null, List.of(Intervals.utc(750, 4000)));
    Assertions.assertEquals(List.of(Intervals.utc(750, 2000), Intervals.utc(3000, 4000)), filtered.getIntervals());
  }

  @Test
  public void testEmptySourceAndEmptyScope() throws IOException
  {
    boundaryResponse.set("[]");
    Assertions.assertEquals(0, source(null).estimateNumSplits(null, null));
    requests.clear();
    Assertions.assertEquals(0, source(List.of()).estimateNumSplits(null, null));
    Assertions.assertTrue(requests.isEmpty());
  }

  @Test
  public void testReadFilterRetriesBeforeExposingRowsAndCleansFiles() throws IOException
  {
    failures.set(1);
    final RemoteDruidInputSource leaf = source(null)
        .withReadFilter(new EqualityFilter("a", ColumnType.STRING, "keep", null), List.of(Intervals.ETERNITY))
        .withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
    final List<InputRow> read = new ArrayList<>();
    try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
      rows.forEachRemaining(read::add);
    }
    Assertions.assertEquals(1, read.size());
    Assertions.assertEquals(0, read.get(0).getTimestampFromEpoch());
    Assertions.assertEquals("keep", read.get(0).getRaw("a"));
    Assertions.assertEquals(10L, read.get(0).getMetric("bytes").longValue());
    Assertions.assertEquals(2, requests.size());
    Assertions.assertEquals(leaf.getFilter(), mapper.treeToValue(requests.get(1).path("filter"), DimFilter.class));
    Assertions.assertEquals(List.of(requests.get(0).path("context").path("queryId").asText()), canceledQueries);
    Assertions.assertFalse(requests.get(1).path("legacy").asBoolean(true));
    Assertions.assertFalse(requests.get(1).path("context").path("returnPartialResults").asBoolean(true));
    Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
  }

  @Test
  public void testRejectMalformedScanWithoutRowsOrLeakedData() throws IOException
  {
    for (final String invalid : List.of(
        "[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[0,\"keep\",10]]}",
        "[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[0,\"SECRET_VALUE\"]]}]",
        "[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[3600000,\"keep\",10]]}]",
        "[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[null,\"keep\",10]]}]"
    )) {
      scanResponse.set(invalid);
      final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
      try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
        final UncheckedIOException exception = Assertions.assertThrows(UncheckedIOException.class, rows::hasNext);
        Assertions.assertFalse(exception.toString().contains("SECRET_VALUE"));
      }
      Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
    }
  }

  @Test
  public void testHttpErrorsAndResponseLimitDoNotRetry() throws IOException
  {
    status.set(401);
    Assertions.assertThrows(IOException.class, () -> source(null).estimateNumSplits(null, null));
    Assertions.assertEquals(1, requests.size());
    requests.clear();
    canceledQueries.clear();
    status.set(200);
    config = config(8, 2);
    final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
    try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
      Assertions.assertThrows(UncheckedIOException.class, rows::hasNext);
    }
    Assertions.assertEquals(1, requests.size());
    Assertions.assertEquals(List.of(requests.get(0).path("context").path("queryId").asText()), canceledQueries);
    Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
  }

  @Test
  public void testPrimitiveArraysAndNullsArePreserved() throws IOException
  {
    scanResponse.set("[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[0,[\"keep\",null,\"other\"],null]]}]");
    final InputRowSchema arraySchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(List.of(
            new AutoTypeColumnSchema("a", ColumnType.STRING_ARRAY, null),
            new LongDimensionSchema("bytes")
        )),
        ColumnsFilter.all()
    );
    final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
    try (final CloseableIterator<InputRow> rows = leaf.reader(arraySchema, null, temporaryFolder.getRoot()).read()) {
      Assertions.assertTrue(rows.hasNext());
      final InputRow row = rows.next();
      Assertions.assertEquals(Arrays.asList("keep", null, "other"), row.getRaw("a"));
      Assertions.assertNull(row.getRaw("bytes"));
      Assertions.assertFalse(rows.hasNext());
    }
    Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
  }

  @Test
  public void testLargeRowsFailBeforeExposureAndCleanFiles() throws IOException
  {
    config = config(4 * 1024 * 1024, 2);
    for (final String largeValue : List.of(
        "[" + String.join(",", java.util.Collections.nCopies(RemoteDruidInputSourceClient.MAX_ROW_TOKENS, "0")) + "]",
        "\"" + "x".repeat(RemoteDruidInputSourceClient.MAX_ROW_BYTES + 1) + "\"",
        " ".repeat(RemoteDruidInputSourceClient.MAX_ROW_BYTES) + "0",
        "[".repeat(65) + "0" + "]".repeat(65)
    )) {
      requests.clear();
      scanResponse.set("[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":[[0,\"keep\",10],[0,"
                       + largeValue + ",10]]}]");
      final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
      try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
        Assertions.assertThrows(UncheckedIOException.class, rows::hasNext);
      }
      Assertions.assertEquals(1, requests.size());
      Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
    }
  }

  @Test
  public void testLargeSchemaRequiresExplicitExtend() throws IOException
  {
    config = config(4 * 1024 * 1024, 2);
    metadataResponse.set(metadataResponse.get() + " ".repeat(RemoteDruidInputSourceClient.MAX_METADATA_BYTES));
    final IOException exception = Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertTrue(exception.getMessage().contains("explicit EXTEND"));
    Assertions.assertEquals(1, requests.size());
  }

  @Test
  public void testNormalizedRowsAreAlsoBounded() throws IOException
  {
    scanResponse.set("[{\"columns\":[\"__time\",\"a\",\"bytes\"],\"events\":["
                     + String.join(",", java.util.Collections.nCopies(20, "[0,\"keep\",10]")) + "]}]");
    config = config(StringUtils.toUtf8(scanResponse.get()).length, 2);
    final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
    try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
      Assertions.assertThrows(UncheckedIOException.class, rows::hasNext);
    }
    Assertions.assertEquals(1, requests.size());
    Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
  }

  @Test
  public void testInterruptedDiscovery()
  {
    Thread.currentThread().interrupt();
    try {
      Assertions.assertThrows(InterruptedIOException.class, () -> source(null).estimateNumSplits(null, null));
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertTrue(requests.isEmpty());
    }
    finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testInterruptedAfterDownloadStopsValidation() throws IOException
  {
    final AtomicInteger cancellations = new AtomicInteger();
    final RemoteDruidAuthentication authentication = (final int connectTimeout, final int readTimeout) ->
        (final URI endpoint, final String method, final byte[] body) -> new RemoteDruidHttpClient.Response()
        {
          @Override
          public int status()
          {
            return 200;
          }

          @Override
          public InputStream body()
          {
            return new ByteArrayInputStream(StringUtils.toUtf8(boundaryResponse.get()))
            {
              @Override
              public void close() throws IOException
              {
                super.close();
                Thread.currentThread().interrupt();
              }
            };
          }

          @Override
          public void close()
          {
            if ("DELETE".equals(method)) {
              cancellations.incrementAndGet();
            }
          }
        };
    final RemoteDruidConnection interrupted = new RemoteDruidConnection(
        config.getEndpoint(), authentication, null, null, null, 2
    );
    try (final RemoteDruidInputSourceClient client = new RemoteDruidInputSourceClient(interrupted, mapper)) {
      Assertions.assertThrows(InterruptedIOException.class, () -> client.timeBoundary("events"));
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertEquals(1, cancellations.get());
    }
    finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testParserRefillsCheckInterruptionInsideStringsAndWhitespace() throws IOException
  {
    for (final String response : List.of("[\"" + "x".repeat(100_000) + "\"]", "[]" + " ".repeat(100_000))) {
      final InputStream interrupted = new ByteArrayInputStream(StringUtils.toUtf8(response))
      {
        @Override
        public synchronized int read(final byte[] bytes, final int offset, final int length)
        {
          final int read = super.read(bytes, offset, length);
          if (pos >= 8192) {
            Thread.currentThread().interrupt();
          }
          return read;
        }
      };
      try (final InputStream in = new RemoteDruidInputSourceClient.InterruptibleInputStream(interrupted);
           final JsonParser parser = mapper.getFactory().createParser(in)) {
        // No token-level interruption checks: the stream must stop Jackson's internal skipping/refill loop.
        Assertions.assertThrows(InterruptedIOException.class, () -> {
          parser.nextToken();
          parser.skipChildren();
          parser.nextToken();
        });
        Assertions.assertTrue(Thread.currentThread().isInterrupted());
      }
      finally {
        Thread.interrupted();
      }
    }
  }

  @Test
  public void testConfigurationRejectsCredentialUrlsAndInvalidLimits()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidConnection(
        URI.create("http://secret@localhost/druid/v2"), null, null, null, null, null
    ));
    Assertions.assertThrows(IllegalArgumentException.class, () -> config(0, 1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> config(100, -1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        config,
        "events",
        null,
        0L,
        null,
        false,
        new HttpInputSourceConfig(null, null),
        mapper
    ));
  }
  @Test
  public void testSchemaDiscoveryAndUnsupportedMetadata() throws IOException
  {
    final RowSignature signature = source(null).discoverSchema();
    Assertions.assertEquals(List.of("__time", "a", "values"), signature.getColumnNames());
    Assertions.assertEquals(ColumnType.STRING, signature.getColumnType("a").orElseThrow());
    Assertions.assertEquals(ColumnType.LONG_ARRAY, signature.getColumnType("values").orElseThrow());
    Assertions.assertTrue(requests.get(0).path("merge").asBoolean());
    Assertions.assertEquals(Intervals.ETERNITY.toString(), requests.get(0).path("intervals").get(0).asText());
    for (final String invalid : List.of(
        "[]",
        "[{\"columns\":{\"__time\":{\"type\":\"STRING\"}}}]",
        "[{\"columns\":{\"__time\":{\"type\":\"LONG\"},\"a\":{\"type\":\"COMPLEX<hyperUnique>\"}}}]",
        "[{\"columns\":{\"__time\":{\"type\":\"LONG\"},\"a\":{\"type\":\"STRING\",\"errorMessage\":\"mixed types\"}}}]"
    )) {
      metadataResponse.set(invalid);
      Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    }
  }

  @Test
  public void testHttpProtocolConfiguration()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        config,
        "events",
        null,
        null,
        null,
        false,
        new HttpInputSourceConfig(java.util.Set.of("https"), null),
        mapper
    ));
    final RemoteDruidConnection file = new RemoteDruidConnection(
        URI.create("file://localhost/tmp/data"), null, null, null, null, null
    );
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        file,
        "events",
        null,
        null,
        null,
        false,
        new HttpInputSourceConfig(java.util.Set.of("file"), null),
        mapper
    ));
    Assertions.assertDoesNotThrow(() -> new RemoteDruidInputSource(
        new RemoteDruidConnection(URI.create("HTTPS://source.example"), null, null, null, null, null),
        "events",
        null,
        null,
        null,
        false,
        new HttpInputSourceConfig(null, null),
        mapper
    ));
    Assertions.assertEquals(URI.create("http://localhost:" + server.getAddress().getPort() + "/druid/v2"), config.getEndpoint());
  }

  @Test
  public void testProviderClientClosedOnDiscoveryAndRead() throws IOException
  {
    final AtomicInteger closedClients = new AtomicInteger();
    final RemoteDruidAuthentication basic = config.getAuthentication();
    final RemoteDruidAuthentication observed = (final int connectTimeout, final int readTimeout) -> new RemoteDruidHttpClient()
    {
      private final RemoteDruidHttpClient delegate = basic.createClient(connectTimeout, readTimeout);

      @Override
      public Response execute(final URI endpoint, final String method, final byte[] body) throws IOException
      {
        return delegate.execute(endpoint, method, body);
      }

      @Override
      public void close()
      {
        delegate.close();
        closedClients.incrementAndGet();
      }
    };
    config = new RemoteDruidConnection(config.getEndpoint(), observed, 1000, 1000, 1024L * 1024, 0);
    source(null).discoverSchema();
    Assertions.assertEquals(1, closedClients.get());
    metadataResponse.set("[]");
    Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertEquals(2, closedClients.get());
    Assertions.assertEquals(2, source(null).estimateNumSplits(null, null));
    Assertions.assertEquals(3, closedClients.get());
    boundaryResponse.set("[{}]");
    Assertions.assertThrows(IOException.class, () -> source(null).estimateNumSplits(null, null));
    Assertions.assertEquals(4, closedClients.get());
    final RemoteDruidInputSource leaf = source(null).withSplit(new InputSplit<>(Intervals.utc(0, 3_600_000)));
    try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
      Assertions.assertEquals("keep", rows.next().getRaw("a"));
    }
    Assertions.assertEquals(5, closedClients.get());
    status.set(401);
    try (final CloseableIterator<InputRow> rows = leaf.reader(schema(), null, temporaryFolder.getRoot()).read()) {
      Assertions.assertThrows(UncheckedIOException.class, rows::hasNext);
    }
    Assertions.assertEquals(6, closedClients.get());
    Assertions.assertEquals(0, temporaryFolder.getRoot().list().length);
  }

}
