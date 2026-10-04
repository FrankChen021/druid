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
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler.TrafficCop;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RemoteDruidFrameInputSourceTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final List<Long> requestedOffsets = new CopyOnWriteArrayList<>();
  private final AtomicInteger releasedSessions = new AtomicInteger();
  private final AtomicInteger submissionAttempts = new AtomicInteger();
  private final List<String> submissionRequestIds = new CopyOnWriteArrayList<>();
  private final AtomicInteger interruptedPartitionResponses = new AtomicInteger();
  private final AtomicBoolean interruptedPartitionResponseOnce = new AtomicBoolean();
  private final AtomicBoolean failSessionStatusOnce = new AtomicBoolean();
  private final AtomicBoolean rejectSubmissionProtocolOnce = new AtomicBoolean();
  private final AtomicBoolean loseSubmissionAcknowledgementOnce = new AtomicBoolean();
  private final AtomicReference<Map<String, List<String>>> manifestColumnMapping = new AtomicReference<>();
  private final AtomicInteger manifestProtocolVersion = new AtomicInteger(RemoteDruidFrameInputSource.PROTOCOL_VERSION);
  private HttpServer server;
  private byte[] frameFileBytes;
  private RowSignature signature;
  private RemoteDruidConnection connection;

  @BeforeEach
  public void setUp() throws IOException
  {
    signature = RowSignature.builder()
                            .add("__time", org.apache.druid.segment.column.ColumnType.LONG)
                            .add("value", org.apache.druid.segment.column.ColumnType.STRING)
                            .add("cnt", org.apache.druid.segment.column.ColumnType.LONG)
                            .add("tags", org.apache.druid.segment.column.ColumnType.STRING_ARRAY)
                            .build();
    final List<MapBasedRow> sourceRows = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      sourceRows.add(new MapBasedRow(
          i,
          Map.of(
              "__time",
              (long) i,
              "value",
              "row-value-" + i,
              "cnt",
              (long) i,
              "tags",
              new Object[]{"tag-" + (i % 3), "shared"}
          )
      ));
    }
    final RowBasedCursorFactory<MapBasedRow> cursorFactory = new RowBasedCursorFactory<>(
        Sequences.simple(sourceRows),
        RowAdapters.standardRow(),
        signature
    );
    final var frameFile = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .frameType(FrameType.latestRowBased())
                            .maxRowsPerFrame(10)
                            .frames(),
        temporaryFolder.newFile()
    );
    frameFileBytes = Files.readAllBytes(frameFile.toPath());

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2/sql/remote-frames", exchange -> {
      try (exchange) {
        Assertions.assertEquals(
            "Basic cmVhZGVyOnNlY3JldA==",
            exchange.getRequestHeaders().getFirst("Authorization")
        );
        final String path = exchange.getRequestURI().getPath();
        if ("POST".equals(exchange.getRequestMethod()) && path.endsWith("/remote-frames")) {
          if (rejectSubmissionProtocolOnce.compareAndSet(true, false)) {
            respondJson(exchange, 409, Map.of("error", "The requested remote frame protocol version is not supported"));
            return;
          }
          final JsonNode submission = mapper.readTree(exchange.getRequestBody());
          submissionAttempts.incrementAndGet();
          submissionRequestIds.add(submission.path("clientRequestId").asText());
          final byte[] submissionResponse = mapper.writeValueAsBytes(Map.of("queryId", "session-1", "state", "submitted"));
          if (loseSubmissionAcknowledgementOnce.compareAndSet(true, false)) {
            exchange.sendResponseHeaders(202, submissionResponse.length + 32);
            exchange.getResponseBody().write(submissionResponse);
            exchange.getResponseBody().flush();
            return;
          }
          respondJson(exchange, 202, Map.of("queryId", "session-1", "state", "submitted"));
        } else if ("GET".equals(exchange.getRequestMethod()) && path.endsWith("/session-1")) {
          if (failSessionStatusOnce.compareAndSet(true, false)) {
            exchange.sendResponseHeaders(503, -1);
            return;
          }
          final Map<String, Object> manifest = new java.util.LinkedHashMap<>(Map.of(
              "protocolVersion",
              manifestProtocolVersion.get(),
              "frameFileVersion",
              RemoteDruidFrameInputSource.FRAME_FILE_VERSION,
              "signature",
              signature,
              "partitions",
              List.of(Map.of("id", "partition-1", "attemptId", "attempt-1"))
          ));
          if (manifestColumnMapping.get() != null) {
            manifest.put("columnMapping", manifestColumnMapping.get());
          }
          respondJson(
              exchange,
              200,
              Map.of("queryId", "session-1", "state", "ready", "manifest", manifest)
          );
        } else if ("GET".equals(exchange.getRequestMethod()) && path.endsWith("/partitions/partition-1")) {
          final long offset = Long.parseLong(queryParameter(exchange.getRequestURI().getRawQuery(), "offset"));
          requestedOffsets.add(offset);
          final int start = Math.toIntExact(offset);
          final int end = Math.min(frameFileBytes.length, start + 8192);
          final byte[] chunk = Arrays.copyOfRange(frameFileBytes, start, end);
          if (chunk.length == 0) {
            exchange.getResponseHeaders().set("X-Druid-Frame-Last-Fetch", "yes");
          }
          exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
          if (offset > 0 && interruptedPartitionResponseOnce.compareAndSet(true, false) && chunk.length > 1) {
            interruptedPartitionResponses.incrementAndGet();
            final int partialLength = chunk.length / 2;
            exchange.sendResponseHeaders(200, chunk.length);
            exchange.getResponseBody().write(chunk, 0, partialLength);
            exchange.getResponseBody().flush();
          } else {
            exchange.sendResponseHeaders(200, chunk.length);
            exchange.getResponseBody().write(chunk);
          }
        } else if ("POST".equals(exchange.getRequestMethod()) && path.endsWith("/lease")) {
          respondJson(exchange, 200, true);
        } else if ("DELETE".equals(exchange.getRequestMethod())) {
          releasedSessions.incrementAndGet();
          exchange.sendResponseHeaders(202, -1);
        } else {
          exchange.sendResponseHeaders(404, -1);
        }
      }
    });
    server.start();

    connection = new RemoteDruidConnection(
        URI.create("http://localhost:" + server.getAddress().getPort() + "/druid/v2"),
        new RemoteDruidAuthentication.Basic("reader", new DefaultPasswordProvider("secret")),
        5_000,
        5_000,
        16L * 1024 * 1024,
        1
    );
    mapper.setInjectableValues(new InjectableValues.Std()
                                  .addValue(HttpInputSourceConfig.class, new HttpInputSourceConfig(null, null))
                                  .addValue(ObjectMapper.class, mapper));
  }

  @AfterEach
  public void tearDown()
  {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testRetriesReadFromPinnedAttemptUntilExplicitRelease() throws IOException
  {
    final RemoteDruidFrameInputSource querySource = new RemoteDruidFrameInputSource(
        connection,
        "SELECT value FROM source",
        "request-1",
        Map.of(),
        signature,
        null,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    );

    final InputSplit<RemoteDruidFrameInputSource.PartitionSplit> partition;
    try (Stream<InputSplit<RemoteDruidFrameInputSource.PartitionSplit>> splits = querySource.createSplits(null, null)) {
      partition = splits.findFirst().orElseThrow();
    }
    final RemoteDruidFrameInputSource boundSource = querySource.withSplit(partition);
    interruptedPartitionResponseOnce.set(true);
    final List<InputRow> rows = new ArrayList<>();
    final File readerDirectory = temporaryFolder.newFolder("reader");
    try (CloseableIterator<InputRow> iterator = boundSource.reader(inputSchema(), null, readerDirectory).read()) {
      iterator.forEachRemaining(rows::add);
    }

    Assertions.assertFalse(rows.isEmpty());
    Assertions.assertEquals(500, rows.size());
    Assertions.assertEquals(1, interruptedPartitionResponses.get());
    Assertions.assertEquals("row-value-0", rows.get(0).getRaw("value"));
    Assertions.assertEquals(499L, rows.get(499).getMetric("cnt").longValue());
    Assertions.assertEquals(List.of("tag-0", "shared"), rows.get(0).getRaw("tags"));
    Assertions.assertTrue(requestedOffsets.size() > 2);
    Assertions.assertEquals(0L, requestedOffsets.get(0));
    Assertions.assertTrue(requestedOffsets.get(1) > 0, "the next read must resume at the accepted file offset");
    Assertions.assertTrue(
        requestedOffsets.get(2) >= requestedOffsets.get(1),
        "a retried read must resume at or after the last accepted file offset"
    );
    for (int i = 3; i < requestedOffsets.size(); i++) {
      Assertions.assertTrue(requestedOffsets.get(i) > requestedOffsets.get(i - 1));
    }
    Assertions.assertEquals(0, releasedSessions.get());
    Assertions.assertEquals(0, readerDirectory.list().length);

    final List<InputRow> retriedRows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = boundSource.reader(inputSchema(), null, readerDirectory).read()) {
      Assertions.assertEquals(0, releasedSessions.get());
      iterator.forEachRemaining(retriedRows::add);
    }
    Assertions.assertEquals(500, retriedRows.size());
    Assertions.assertEquals(0, releasedSessions.get());

    querySource.releaseSession("session-1");
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testStreamingResponseBackpressureKeepsQueueBoundedAndAbortsOnEarlyClose()
  {
    final RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler handler =
        new RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler();
    final TrafficCop trafficCop = mock(TrafficCop.class);
    ClientResponse<InputStreamFullResponseHolder> response = handler.handleResponse(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
        trafficCop
    );
    long chunkNumber = 0;
    while (handler.getQueuedBytes() < RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.MAX_QUEUED_BYTES) {
      final DefaultHttpContent chunk = new DefaultHttpContent(
          Unpooled.wrappedBuffer(new byte[RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.READ_SIZE])
      );
      try {
        response = handler.handleChunk(response, chunk, ++chunkNumber);
      }
      finally {
        chunk.release();
      }
    }

    Assertions.assertFalse(response.isContinueReading(), "the transport must suspend when the bounded queue is full");
    Assertions.assertEquals(
        RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.MAX_QUEUED_BYTES,
        handler.getQueuedBytes()
    );
    handler.resume(RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.READ_SIZE);
    Assertions.assertEquals(
        RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.MAX_QUEUED_BYTES
        - RemoteDruidHttpClient.DruidHttpClient.StreamingResponseHandler.READ_SIZE,
        handler.getQueuedBytes()
    );
    verify(trafficCop).resume(chunkNumber);

    handler.close();
    verify(trafficCop).abort();
  }

  @Test
  public void testMapsPhysicalFrameColumnsToAliasedTargetSignature() throws IOException
  {
    manifestColumnMapping.set(Map.of(
        "__time", List.of("__time"),
        "value", List.of("renamed_value"),
        "cnt", List.of("cnt"),
        "tags", List.of("tags")
    ));
    final RowSignature outputSignature = RowSignature.builder()
                                                     .add("cnt", org.apache.druid.segment.column.ColumnType.LONG)
                                                     .add("__time", org.apache.druid.segment.column.ColumnType.LONG)
                                                     .add("renamed_value", org.apache.druid.segment.column.ColumnType.STRING)
                                                     .add("tags", org.apache.druid.segment.column.ColumnType.STRING_ARRAY)
                                                     .build();
    final RemoteDruidFrameInputSource querySource = querySource(outputSignature);
    final InputSplit<RemoteDruidFrameInputSource.PartitionSplit> partition;
    try (Stream<InputSplit<RemoteDruidFrameInputSource.PartitionSplit>> splits = querySource.createSplits(null, null)) {
      partition = splits.findFirst().orElseThrow();
    }

    final RemoteDruidFrameInputSource boundSource = querySource.withSplit(partition);
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = boundSource.reader(inputSchema("renamed_value"), null,
                                                                   temporaryFolder.newFolder("mapped-reader")).read()) {
      iterator.forEachRemaining(rows::add);
    }

    Assertions.assertEquals(500, rows.size());
    Assertions.assertEquals(0L, rows.get(0).getTimestampFromEpoch());
    Assertions.assertEquals("row-value-0", rows.get(0).getRaw("renamed_value"));
    Assertions.assertNull(rows.get(0).getRaw("value"));
    Assertions.assertEquals(499L, rows.get(499).getMetric("cnt").longValue());
  }


  @Test
  public void testRetriesAcceptedSubmissionWithTheSameRequestIdWhenAcknowledgementIsLost() throws IOException
  {
    loseSubmissionAcknowledgementOnce.set(true);
    final RemoteDruidFrameInputSource querySource = querySource(signature);

    try (Stream<InputSplit<RemoteDruidFrameInputSource.PartitionSplit>> splits = querySource.createSplits(null, null)) {
      Assertions.assertEquals(1, splits.count());
    }

    Assertions.assertEquals(2, submissionAttempts.get());
    Assertions.assertEquals(List.of("request-1", "request-1"), submissionRequestIds);
    querySource.releaseSession("session-1");
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testReleasesAcceptedSessionWhenStatusPollingFails() throws IOException
  {
    failSessionStatusOnce.set(true);
    final RemoteDruidFrameInputSource querySource = querySource(signature);

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> querySource.createSplits(null, null)
    );

    Assertions.assertTrue(exception.getMessage().contains("HTTP status [503]"));
    Assertions.assertEquals(0, requestedOffsets.size());
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testRejectsUnsupportedManifestProtocolBeforeReadingPartition() throws IOException
  {
    manifestProtocolVersion.set(RemoteDruidFrameInputSource.PROTOCOL_VERSION + 1);
    final RemoteDruidFrameInputSource querySource = querySource(signature);

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> querySource.createSplits(null, null)
    );

    Assertions.assertTrue(exception.getMessage().contains("protocol or FrameFile version"));
    Assertions.assertTrue(requestedOffsets.isEmpty());
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testReportsActionableErrorWhenSourceDoesNotSupportProtocol() throws IOException
  {
    rejectSubmissionProtocolOnce.set(true);
    final RemoteDruidFrameInputSource querySource = querySource(signature);

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> querySource.createSplits(null, null)
    );

    Assertions.assertTrue(exception.getMessage().contains("protocol versions may be incompatible"));
    Assertions.assertTrue(requestedOffsets.isEmpty());
    Assertions.assertEquals(0, releasedSessions.get());
  }

  @Test
  public void testRejectsManifestSignatureMismatchBeforeReadingPartition() throws IOException
  {
    final RowSignature wrongSignature = RowSignature.builder()
                                                    .add("__time", org.apache.druid.segment.column.ColumnType.LONG)
                                                    .add("value", org.apache.druid.segment.column.ColumnType.LONG)
                                                    .add("cnt", org.apache.druid.segment.column.ColumnType.LONG)
                                                    .build();
    final RemoteDruidFrameInputSource querySource = querySource(wrongSignature);

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> querySource.createSplits(null, null)
    );

    Assertions.assertTrue(exception.getMessage().contains("row signature differs"));
    Assertions.assertTrue(requestedOffsets.isEmpty());
    Assertions.assertEquals(1, releasedSessions.get());
  }

  private RemoteDruidFrameInputSource querySource(final RowSignature expectedSignature)
  {
    return new RemoteDruidFrameInputSource(
        connection,
        "SELECT value FROM source",
        "request-1",
        Map.of(),
        expectedSignature,
        null,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null, null),
        mapper
    );
  }

  private InputRowSchema inputSchema()
  {
    return inputSchema("value");
  }

  private InputRowSchema inputSchema(final String valueColumn)
  {
    return new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of(valueColumn))),
        ColumnsFilter.all()
    );
  }

  private void respondJson(final com.sun.net.httpserver.HttpExchange exchange, final int status, final Object entity)
      throws IOException
  {
    final byte[] response = mapper.writeValueAsBytes(entity);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, response.length);
    exchange.getResponseBody().write(response);
  }

  private static String queryParameter(final String query, final String name)
  {
    for (final String parameter : query.split("&")) {
      final String[] pair = parameter.split("=", 2);
      if (pair[0].equals(name)) {
        return pair[1];
      }
    }
    throw new IllegalArgumentException("Missing query parameter " + name);
  }
}
