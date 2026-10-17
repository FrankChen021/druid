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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteDruidInputSourceTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private final List<String> canceledQueries = new CopyOnWriteArrayList<>();
  // Recorded by the source and checked on the test thread, since a failed assertion in a handler only breaks the
  // connection.
  private final List<String> authorizations = new CopyOnWriteArrayList<>();
  private final AtomicInteger status = new AtomicInteger(200);
  private final AtomicInteger unavailableResponses = new AtomicInteger();
  private final AtomicReference<String> metadataResponse = new AtomicReference<>(
      """
      [{"columns":{"__time":{"typeSignature":"LONG"},
                   "a":{"type":"STRING"},
                   "values":{"typeSignature":"ARRAY<LONG>"}}}]
      """
  );
  private HttpServer server;
  private RemoteDruidConnection config;

  @BeforeEach
  public void setUp() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        authorizations.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
        if ("DELETE".equals(exchange.getRequestMethod())) {
          canceledQueries.add(exchange.getRequestURI().getPath().substring("/druid/v2/".length()));
          exchange.sendResponseHeaders(200, -1);
          return;
        }
        final JsonNode query = mapper.readTree(exchange.getRequestBody());
        requests.add(query);
        final String response = metadataResponse.get();
        final byte[] bytes = StringUtils.toUtf8(response);
        if (unavailableResponses.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
          exchange.sendResponseHeaders(503, -1);
          return;
        }
        exchange.sendResponseHeaders(status.get(), bytes.length);
        exchange.getResponseBody().write(bytes);
      }
    });
    server.start();
    config = config(1);
    mapper.setInjectableValues(new InjectableValues.Std()
                                  .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                  .addValue(ObjectMapper.class, mapper)
                                  .addValue(RemoteDruidTlsContext.class, new RemoteDruidTlsContext()));
  }

  @AfterEach
  public void tearDown()
  {
    server.stop(0);
    for (final String authorization : authorizations) {
      Assertions.assertEquals("Basic dXNlcjpzZWNyZXQ=", authorization);
    }
    for (final JsonNode request : requests) {
      Assertions.assertEquals("segmentMetadata", request.path("queryType").asText());
    }
  }

  private RemoteDruidConnection config(final int retries)
  {
    return new RemoteDruidConnection(
        URI.create("http://localhost:" + server.getAddress().getPort()),
        new RemoteDruidAuthentication.Basic("user", new DefaultPasswordProvider("secret")),
        1000,
        1000,
        retries,
        null
    );
  }

  private RemoteDruidInputSource source(final List<Interval> intervals)
  {
    return new RemoteDruidInputSource(
        config,
        "events",
        intervals,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    );
  }

  private RemoteDruidInputSource inputSource(
      final RemoteDruidConnection connection,
      @Nullable final String engine,
      @Nullable final Period splitDuration,
      @Nullable final String includeSegmentSource,
      @Nullable final Boolean declaredSchema,
      final HttpInputSourceConfig httpConfig
  )
  {
    return new RemoteDruidInputSource(
        connection,
        "events",
        null,
        engine,
        splitDuration,
        includeSegmentSource,
        declaredSchema,
        httpConfig,
        mapper
    );
  }

  private RemoteDruidInputSource marker(@Nullable final String engine, @Nullable final String includeSegmentSource)
  {
    return inputSource(config, engine, null, includeSegmentSource, null, new HttpInputSourceConfig(null, null));
  }

  @Test
  public void testLargeSchemaRequiresExplicitExtend() throws IOException
  {
    config = config(2);
    metadataResponse.set(metadataResponse.get() + " ".repeat(RemoteDruidMetadataClient.MAX_METADATA_BYTES));
    final IOException exception = Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertTrue(exception.getMessage().contains("explicit EXTEND"));
    Assertions.assertEquals(1, requests.size());
    // The source was streaming when the client gave up, so the query is canceled.
    Assertions.assertEquals(List.of(requests.get(0).path("context").path("queryId").asText()), canceledQueries);
  }

  @Test
  public void testInterruptedDiscovery()
  {
    Thread.currentThread().interrupt();
    try {
      Assertions.assertThrows(InterruptedIOException.class, () -> source(null).discoverSchema());
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertTrue(requests.isEmpty());
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
      try (final InputStream in = new RemoteDruidMetadataClient.InterruptibleInputStream(interrupted);
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
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> connection(URI.create("http://secret@localhost/druid/v2"))
    );
    Assertions.assertThrows(IllegalArgumentException.class, () -> config(-1));
    final HttpInputSourceConfig httpConfig = new HttpInputSourceConfig(null, null);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> inputSource(config, "unknown", null, null, null, httpConfig)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> inputSource(config, null, Period.ZERO, null, null, httpConfig)
    );
  }

  @Test
  public void testIncludeSegmentSource()
  {
    Assertions.assertEquals("NONE", sourceContext(null, null).get("includeSegmentSource"));
    Assertions.assertEquals("REALTIME", sourceContext(null, "REALTIME").get("includeSegmentSource"));
    Assertions.assertFalse(sourceContext("native", null).containsKey("includeSegmentSource"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> marker("native", "NONE"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> marker(null, "ALL"));
  }

  @Test
  public void testSchemaIsDeclaredUnlessDiscovered() throws IOException
  {
    // EXTERN specs carry their signature, so a spec without the flag has a declared schema.
    Assertions.assertTrue(marker(null, null).isDeclaredSchema());
    final RemoteDruidInputSource discovered =
        inputSource(config, null, null, null, false, new HttpInputSourceConfig(null, null));
    Assertions.assertFalse(
        ((RemoteDruidInputSource) mapper.readValue(mapper.writeValueAsString(discovered), InputSource.class))
            .isDeclaredSchema()
    );
  }

  private Map<String, Object> sourceContext(@Nullable final String engine, @Nullable final String includeSegmentSource)
  {
    return marker(engine, includeSegmentSource)
        .toSqlInputSource("SELECT 1", false, RowSignature.empty(), List.of(), Map.of())
        .getContext();
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
    for (final Map.Entry<String, String> invalid : Map.of(
        "[]",
        "Invalid remote Druid response structure",
        "[{\"columns\":{\"__time\":{\"type\":\"STRING\"}}}]",
        "Invalid remote Druid response structure",
        "[{\"columns\":{\"__time\":{\"type\":\"LONG\"},\"a\":{\"type\":\"COMPLEX<hyperUnique>\"}}}]",
        "Unsupported remote Druid column type",
        "[{\"columns\":{\"__time\":{\"type\":\"LONG\"},\"a\":{\"type\":\"STRING\",\"errorMessage\":\"mixed types\"}}}]",
        "column metadata is ambiguous"
    ).entrySet()) {
      metadataResponse.set(invalid.getKey());
      final IOException e = Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
      Assertions.assertTrue(e.getMessage().contains(invalid.getValue()), invalid.getKey() + " -> " + e.getMessage());
    }
  }

  @Test
  public void testFailedMetadataQueryIsNotCanceled()
  {
    status.set(400);
    Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    // The source rejected the query, so there is nothing to cancel or retry.
    Assertions.assertEquals(1, requests.size());
    Assertions.assertEquals(List.of(), canceledQueries);
  }

  @Test
  public void testSqlEndpointIsNormalized()
  {
    Assertions.assertEquals(
        URI.create("https://source.example/druid/v2"),
        connection(URI.create("https://source.example/druid/v2/sql/")).getEndpoint()
    );
  }

  @Test
  public void testHttpProtocolRestrictions()
  {
    // The configured protocols apply, and only HTTP(S) is accepted even if others are allowed.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> inputSource(config, null, null, null, null, new HttpInputSourceConfig(Set.of("https"), null))
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> inputSource(
            connection(URI.create("file://localhost/tmp/data")),
            null,
            null,
            null,
            null,
            new HttpInputSourceConfig(Set.of("file"), null)
        )
    );
    // Schemes are not case-sensitive.
    Assertions.assertDoesNotThrow(
        () -> inputSource(
            connection(URI.create("HTTPS://source.example")),
            null,
            null,
            null,
            null,
            new HttpInputSourceConfig(null, null)
        )
    );
  }

  @Test
  public void testRetriesUnavailableMetadataQueries() throws IOException
  {
    unavailableResponses.set(1);
    Assertions.assertEquals(List.of("__time", "a", "values"), source(null).discoverSchema().getColumnNames());
    // The unavailable source never accepted the first query, so nothing is canceled.
    Assertions.assertEquals(2, requests.size());
    Assertions.assertEquals(List.of(), canceledQueries);
  }

  private static RemoteDruidConnection connection(final URI endpoint)
  {
    return new RemoteDruidConnection(endpoint, null, null, null, null, null);
  }

  @Test
  public void testProviderClientClosedOnDiscovery() throws IOException
  {
    final AtomicInteger closedClients = new AtomicInteger();
    final RemoteDruidAuthentication basic = config.getAuthentication();
    final RemoteDruidAuthentication observed =
        (final int connectTimeout, final int readTimeout, final SSLContext sslContext) -> new RemoteDruidHttpClient()
    {
      private final RemoteDruidHttpClient delegate = basic.createClient(connectTimeout, readTimeout, sslContext);

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
    config = new RemoteDruidConnection(config.getEndpoint(), observed, 1000, 1000, 0, null);
    source(null).discoverSchema();
    Assertions.assertEquals(1, closedClients.get());
    metadataResponse.set("[]");
    Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertEquals(2, closedClients.get());
  }
}
