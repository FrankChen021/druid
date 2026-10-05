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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteDruidInputSourceTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private final List<String> canceledQueries = new CopyOnWriteArrayList<>();
  private final AtomicInteger status = new AtomicInteger(200);
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
        Assertions.assertEquals("segmentMetadata", query.path("queryType").asText());
        final String response = metadataResponse.get();
        final byte[] bytes = StringUtils.toUtf8(response);
        exchange.sendResponseHeaders(status.get(), bytes.length);
        exchange.getResponseBody().write(bytes);
      }
    });
    server.start();
    config = config(1);
    mapper.setInjectableValues(new InjectableValues.Std()
                                  .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                  .addValue(ObjectMapper.class, mapper));
  }

  @AfterEach
  public void tearDown()
  {
    server.stop(0);
  }

  private RemoteDruidConnection config(final int retries)
  {
    return new RemoteDruidConnection(
        URI.create("http://localhost:" + server.getAddress().getPort()),
        new RemoteDruidAuthentication.Basic("user", new DefaultPasswordProvider("secret")),
        1000, 1000, retries
    );
  }

  private RemoteDruidInputSource source(final List<Interval> intervals)
  {
    return new RemoteDruidInputSource(config, "events", intervals, null, null,
                                      new HttpInputSourceConfig(null, null), mapper);
  }

  @Test
  public void testLargeSchemaRequiresExplicitExtend() throws IOException
  {
    config = config(2);
    metadataResponse.set(metadataResponse.get() + " ".repeat(RemoteDruidMetadataClient.MAX_METADATA_BYTES));
    final IOException exception = Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertTrue(exception.getMessage().contains("explicit EXTEND"));
    Assertions.assertEquals(1, requests.size());
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
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidConnection(
        URI.create("http://secret@localhost/druid/v2"), null, null, null, null
    ));
    Assertions.assertThrows(IllegalArgumentException.class, () -> config(-1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        config,
        "events",
        null,
        "unknown",
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    ));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        config,
        "events",
        null,
        null,
        org.joda.time.Period.ZERO,
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
        new HttpInputSourceConfig(java.util.Set.of("https"), null),
        mapper
    ));
    final RemoteDruidConnection file = new RemoteDruidConnection(
        URI.create("file://localhost/tmp/data"), null, null, null, null
    );
    Assertions.assertThrows(IllegalArgumentException.class, () -> new RemoteDruidInputSource(
        file,
        "events",
        null,
        null,
        null,
        new HttpInputSourceConfig(java.util.Set.of("file"), null),
        mapper
    ));
    Assertions.assertDoesNotThrow(() -> new RemoteDruidInputSource(
        new RemoteDruidConnection(URI.create("HTTPS://source.example"), null, null, null, null),
        "events",
        null,
        null,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    ));
    Assertions.assertEquals(URI.create("http://localhost:" + server.getAddress().getPort() + "/druid/v2"), config.getEndpoint());
  }

  @Test
  public void testProviderClientClosedOnDiscovery() throws IOException
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
    config = new RemoteDruidConnection(config.getEndpoint(), observed, 1000, 1000, 0);
    source(null).discoverSchema();
    Assertions.assertEquals(1, closedClients.get());
    metadataResponse.set("[]");
    Assertions.assertThrows(IOException.class, () -> source(null).discoverSchema());
    Assertions.assertEquals(2, closedClients.get());
  }
}
