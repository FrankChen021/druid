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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.sun.net.httpserver.HttpServer;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.client.TestHttpClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.rpc.ControllerResource;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.msq.rpc.WorkerResource;
import org.apache.druid.msq.sql.LiveFramesRequestContext;
import org.apache.druid.msq.sql.resources.RemoteDruidFrameResource;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestControllerContext;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/// Exercises the SQL planner, HTTP native queries, MSQ workers, and target segment publication together.
public class MSQRemoteDruidInputSourceTest extends MSQTestBase
{
  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private final List<String> remoteSourceSql = new CopyOnWriteArrayList<>();
  private final List<Map<String, Object>> remoteSourceContexts = new CopyOnWriteArrayList<>();
  private final List<String> remotePartitionReadsFromStart = new CopyOnWriteArrayList<>();
  private final AtomicInteger remoteFrameSubmissions = new AtomicInteger();
  private final AtomicInteger releasedSessions = new AtomicInteger();
  private final AtomicReference<RowSignature> remoteFrameSignature = new AtomicReference<>();
  private final AtomicReference<Map<String, byte[]>> remoteFrameFiles = new AtomicReference<>(Map.of());
  private final AtomicReference<Throwable> remoteSourceError = new AtomicReference<>();
  private final AtomicBoolean useProductionGateway = new AtomicBoolean();
  private final AtomicReference<ControllerImpl> productionController = new AtomicReference<>();
  private final AtomicReference<MSQTestControllerContext> productionControllerContext = new AtomicReference<>();
  private final AtomicReference<FutureTask<Response>> productionSubmitFuture = new AtomicReference<>();
  private final AtomicReference<RemoteDruidFrameResource> productionResource = new AtomicReference<>();
  private final AtomicReference<HttpServletRequest> productionRequest = new AtomicReference<>();
  private final CountDownLatch productionControllerRegistered = new CountDownLatch(1);
  private final AtomicInteger productionWorkerReads = new AtomicInteger();
  private final List<String> productionGatewayPaths = new CopyOnWriteArrayList<>();
  private final List<String> productionSourceSql = new CopyOnWriteArrayList<>();
  private volatile String remoteSourceDefaultTimeZone = "UTC";
  private HttpServer server;
  private String expectedAuthorization;

  @BeforeEach
  public void startSource() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        Assertions.assertEquals(expectedAuthorization, exchange.getRequestHeaders().getFirst("Authorization"));
        if (exchange.getRequestURI().getPath().contains("/sql/remote-frames")) {
          if (useProductionGateway.get()) {
            handleProductionRemoteFrames(exchange);
          } else {
            handleRemoteFrames(exchange);
          }
          return;
        }
        if ("DELETE".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
          return;
        }
        final JsonNode request = objectMapper.readTree(exchange.getRequestBody());
        requests.add(request);
        final Query<?> parsed = objectMapper.treeToValue(request, Query.class);
        // Expose the fixture's three primitive columns in native schema discovery.
        final Query<?> query = parsed instanceof SegmentMetadataQuery metadata
                               ? metadata.withColumns(new ListColumnIncluderator(List.of("__time", "dim1", "cnt"))) : parsed;
        final List<?> result = QueryPlus.wrap(query).run(queryFramework().walker(), ResponseContext.createEmpty()).toList();
        final byte[] response = objectMapper.writeValueAsBytes(result);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
      }
    });
    server.start();
  }

  @AfterEach
  public void stopSource()
  {
    server.stop(0);
  }

  private String external()
  {
    return "TABLE(DRUID(endpoint => 'http://localhost:" + server.getAddress().getPort()
           + "', dataSource => 'foo', splitDurationMillis => 31536000000))"
           + " EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)";
  }

  private String externalAcrossSourceIntervals()
  {
    return "TABLE(DRUID(endpoint => 'http://localhost:" + server.getAddress().getPort()
           + "', dataSource => 'foo', intervals => ARRAY['2000-01-01/2001-01-01', '2001-01-01/2002-01-01']))"
           + " EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)";
  }

  private String externalAcrossSourceIntervalsBasic()
  {
    return "TABLE(DRUID(endpoint => 'http://localhost:" + server.getAddress().getPort()
           + "/druid/v2', dataSource => 'foo', authType => 'basic', username => 'reader', password => 'secret',"
           + " intervals => ARRAY['2000-01-01/2001-01-01', '2001-01-01/2002-01-01']))"
           + " EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)";
  }

  private String externalBasic()
  {
    return "TABLE(DRUID(endpoint => 'http://localhost:" + server.getAddress().getPort()
           + "', dataSource => 'foo', authType => 'basic', username => 'reader', password => 'secret',"
           + " splitDurationMillis => 31536000000))"
           + " EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)";
  }

  private void handleRemoteFrames(final com.sun.net.httpserver.HttpExchange exchange) throws IOException
  {
    final String path = exchange.getRequestURI().getPath();
    if ("POST".equals(exchange.getRequestMethod()) && path.endsWith("/remote-frames")) {
      final JsonNode request = objectMapper.readTree(exchange.getRequestBody());
      final String sourceSql = request.path("sql").asText();
      remoteSourceSql.add(sourceSql);
      final Map<String, Object> requestContext = request.path("context").isObject()
                                                 ? objectMapper.convertValue(
                                                     request.path("context"),
                                                     new TypeReference<>()
                                                     {
                                                     }
                                                 )
                                                 : Map.of();
      remoteSourceContexts.add(requestContext);
      final int submissionNumber = remoteFrameSubmissions.incrementAndGet();
      try {
        final Map<String, Object> sourceQueryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
        sourceQueryContext.put("sqlTimeZone", remoteSourceDefaultTimeZone);
        sourceQueryContext.putAll(requestContext);
        sourceQueryContext.put("sqlQueryId", "remote-source-test-" + submissionNumber);
        final Pair<org.apache.druid.msq.indexing.LegacyMSQSpec,
                   Pair<List<MSQResultsReport.ColumnAndType>, List<Object[]>>> sourceResult =
            testSelectQuery().setSql(sourceSql).setQueryContext(sourceQueryContext).runQueryWithResult();
        if (sourceResult == null) {
          throw new IllegalStateException("Source MSQ SELECT did not return rows");
        }

        final RowSignature signature = MSQResultsReport.ColumnAndType.toRowSignature(sourceResult.rhs.lhs);
        final int timeColumn = signature.indexOf("__time");
        if (timeColumn < 0) {
          throw new IllegalStateException("Source MSQ result did not include __time");
        }
        final List<MapBasedRow> rows = new ArrayList<>();
        for (final Object[] resultRow : sourceResult.rhs.rhs) {
          final Map<String, Object> event = new LinkedHashMap<>();
          for (int column = 0; column < signature.size(); column++) {
            event.put(signature.getColumnName(column), resultRow[column]);
          }
          final Object rawTimestamp = event.get("__time");
          final long timestamp = rawTimestamp instanceof Number
                                ? ((Number) rawTimestamp).longValue()
                                : DateTimes.of(rawTimestamp.toString()).getMillis();
          event.put("__time", timestamp);
          rows.add(new MapBasedRow(timestamp, event));
        }

        final int partitionCount = rows.isEmpty() ? 0 : rows.size() > 1 ? 2 : 1;
        final int rowsPerPartition = partitionCount == 0 ? 1 : (rows.size() + partitionCount - 1) / partitionCount;
        final Map<String, byte[]> frameFiles = new LinkedHashMap<>();
        final File outputDirectory = newTempFolder("remote-frame-result-" + submissionNumber);
        for (int partitionNumber = 0; partitionNumber < partitionCount; partitionNumber++) {
          final int start = partitionNumber * rowsPerPartition;
          final int end = Math.min(rows.size(), start + rowsPerPartition);
          final RowBasedCursorFactory<MapBasedRow> partitionCursorFactory = new RowBasedCursorFactory<>(
              Sequences.simple(rows.subList(start, end)),
              RowAdapters.standardRow(),
              signature
          );
          final File outputFile = new File(outputDirectory, "partition-" + (partitionNumber + 1) + ".frames");
          FrameTestUtil.writeFrameFile(
              FrameSequenceBuilder.fromCursorFactory(partitionCursorFactory)
                                  .frameType(FrameType.latestRowBased())
                                  .frames(),
              outputFile
          );
          frameFiles.put("partition-" + (partitionNumber + 1), Files.readAllBytes(outputFile.toPath()));
        }
        remoteFrameSignature.set(signature);
        remoteFrameFiles.set(Map.copyOf(frameFiles));
        respondJson(exchange, 202, Map.of("queryId", "session-1", "state", "submitted"));
      }
      catch (Exception e) {
        remoteSourceError.set(e);
        respondJson(exchange, 500, Map.of("error", e.getClass().getSimpleName()));
      }
      return;
    }

    if ("GET".equals(exchange.getRequestMethod()) && path.endsWith("/session-1")) {
      respondJson(
          exchange,
          200,
          Map.of(
              "queryId",
              "session-1",
              "state",
              "ready",
              "manifest",
              Map.of(
                  "protocolVersion",
                  1,
                  "frameFileVersion",
                  1,
                  "finalStageNumber",
                  0,
                  "signature",
                  remoteFrameSignature.get(),
                  "partitions",
                  remoteFrameFiles.get().keySet().stream()
                                  .map(partitionId -> Map.of("id", partitionId, "attemptId", "attempt-" + partitionId))
                                  .toList()
              )
          )
      );
    } else if ("GET".equals(exchange.getRequestMethod()) && path.contains("/partitions/")) {
      final String partitionId = path.substring(path.lastIndexOf('/') + 1);
      final byte[] frameBytes = remoteFrameFiles.get().get(partitionId);
      if (frameBytes == null) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      final String attemptId = queryParameter(exchange.getRequestURI().getRawQuery(), "attemptId");
      if (!("attempt-" + partitionId).equals(attemptId)) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      final long offset = queryLong(exchange.getRequestURI().getRawQuery(), "offset");
      if (offset == 0) {
        remotePartitionReadsFromStart.add(partitionId);
      }
      final int start = Math.toIntExact(offset);
      if (start >= frameBytes.length) {
        exchange.getResponseHeaders().set("X-Druid-Frame-Last-Fetch", "yes");
        exchange.sendResponseHeaders(200, -1);
      } else {
        final byte[] chunk = Arrays.copyOfRange(frameBytes, start, Math.min(frameBytes.length, start + 8192));
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
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

  private void handleProductionRemoteFrames(final com.sun.net.httpserver.HttpExchange exchange) throws IOException
  {
    final String path = exchange.getRequestURI().getPath();
    productionGatewayPaths.add(exchange.getRequestMethod() + " " + path);
    final RemoteDruidFrameResource resource = productionResource.get();
    if (resource == null) {
      respondJson(exchange, 500, Map.of("error", "Production remote-frame resource is not configured"));
      return;
    }

    final String basePath = "/druid/v2/sql/remote-frames";
    final String suffix = path.substring(basePath.length());
    if ("POST".equals(exchange.getRequestMethod()) && suffix.isEmpty()) {
      final RemoteDruidFrameResource.RemoteFramesQueryRequest queryRequest = objectMapper.readValue(
          exchange.getRequestBody(),
          RemoteDruidFrameResource.RemoteFramesQueryRequest.class
      );
      productionSourceSql.add(queryRequest.getSql());
      final FutureTask<Response> submission = new FutureTask<>(
          () -> resource.submit(queryRequest, productionRequest.get())
      );
      productionSubmitFuture.set(submission);
      final Thread submissionThread = new Thread(submission, "remote-frame-source-resource-submit");
      submissionThread.setDaemon(true);
      submissionThread.start();
      try {
        if (!productionControllerRegistered.await(30, TimeUnit.SECONDS)) {
          respondJson(exchange, 500, Map.of("error", "Source MSQ controller did not start"));
          return;
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for source MSQ controller", e);
      }

      final ControllerImpl controller = productionController.get();
      respondJson(
          exchange,
          Response.Status.ACCEPTED.getStatusCode(),
          new LiveFramesSession.SessionInfo(controller.queryId(), "submitted", null, null, 0)
      );
      return;
    }

    final String[] pathParts = suffix.split("/");
    if (pathParts.length == 2 && "GET".equals(exchange.getRequestMethod())) {
      writeResourceResponse(exchange, resource.getStatus(decode(pathParts[1]), productionRequest.get()));
      return;
    }
    if (pathParts.length == 4 && "partitions".equals(pathParts[2]) && "GET".equals(exchange.getRequestMethod())) {
      final String query = exchange.getRequestURI().getRawQuery();
      writeResourceResponse(
          exchange,
          resource.readPartition(
              decode(pathParts[1]),
              decode(pathParts[3]),
              decode(queryParameter(query, "attemptId")),
              queryLong(query, "offset"),
              productionRequest.get()
          )
      );
      return;
    }
    if (pathParts.length == 3 && "lease".equals(pathParts[2]) && "POST".equals(exchange.getRequestMethod())) {
      final RemoteDruidFrameResource.LeaseRequest leaseRequest = objectMapper.readValue(
          exchange.getRequestBody(),
          RemoteDruidFrameResource.LeaseRequest.class
      );
      writeResourceResponse(exchange, resource.renewLease(leaseRequest, decode(pathParts[1]), productionRequest.get()));
      return;
    }
    if (pathParts.length == 2 && "DELETE".equals(exchange.getRequestMethod())) {
      final Response response = resource.release(decode(pathParts[1]), productionRequest.get());
      if (response.getStatus() == Response.Status.ACCEPTED.getStatusCode()) {
        releasedSessions.incrementAndGet();
      }
      writeResourceResponse(exchange, response);
      return;
    }
    exchange.sendResponseHeaders(Response.Status.NOT_FOUND.getStatusCode(), -1);
  }

  private void writeResourceResponse(
      final com.sun.net.httpserver.HttpExchange exchange,
      final Response response
  ) throws IOException
  {
    final Object entity = response.getEntity();
    if (entity instanceof StreamingOutput streamingOutput) {
      exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
      final Object lastFetch = response.getMetadata().getFirst(RemoteDruidFrameResource.LAST_FETCH_HEADER);
      if (lastFetch != null) {
        exchange.getResponseHeaders().set(RemoteDruidFrameResource.LAST_FETCH_HEADER, lastFetch.toString());
      }
      exchange.sendResponseHeaders(response.getStatus(), 0);
      streamingOutput.write(exchange.getResponseBody());
      return;
    }
    if (entity == null) {
      exchange.sendResponseHeaders(response.getStatus(), -1);
      return;
    }
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    final byte[] payload = objectMapper.writeValueAsBytes(entity);
    exchange.sendResponseHeaders(response.getStatus(), payload.length);
    exchange.getResponseBody().write(payload);
  }

  private InternalGatewayResponse dispatchInternalRequest(
      final String serviceName,
      final RequestBuilder requestBuilder
  ) throws Exception
  {
    final String requestPath = requestPath(requestBuilder);
    if (requestPath.startsWith("/channels/")) {
      return dispatchWorkerRequest(serviceName, requestPath);
    }

    final String[] parts = requestPath.split("\\?", 2);
    final String route = parts[0];
    final String query = parts.length == 2 ? parts[1] : "";
    final String[] pathParts = route.split("/");
    final ControllerImpl controller = productionController.get();
    if (controller == null) {
      throw new IllegalStateException("Source MSQ controller is not registered");
    }
    final ControllerResource controllerResource = new ControllerResource(
        controller,
        new ResourcePermissionMapper()
        {
          @Override
          public List<ResourceAction> getAdminPermissions()
          {
            return List.of();
          }

          @Override
          public List<ResourceAction> getQueryPermissions(final String queryId)
          {
            return List.of();
          }
        },
        new AuthorizerMapper(Map.of(AuthConfig.ALLOW_ALL_NAME, new AllowAllAuthorizer(null)))
    );
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT);
    final String queryId = decode(pathParts.length > 3 ? pathParts[3] : pathParts[2]);
    final Response response;
    if (pathParts.length == 4 && "GET".equals(requestMethod(requestBuilder)) && "status".equals(pathParts[2])) {
      response = controllerResource.httpGetLiveFramesStatus(queryId, decode(queryParameter(query, "identity")), request);
    } else if (pathParts.length == 4 && "POST".equals(requestMethod(requestBuilder)) && "lease".equals(pathParts[2])) {
      final ControllerResource.LiveFramesLeaseRequest leaseRequest = objectMapper.readValue(
          requestContent(requestBuilder),
          ControllerResource.LiveFramesLeaseRequest.class
      );
      response = controllerResource.httpRenewLiveFramesLease(leaseRequest, queryId, request);
    } else if (pathParts.length == 5 && "GET".equals(requestMethod(requestBuilder)) && "partitions".equals(pathParts[2])) {
      response = controllerResource.httpGetLiveFramesPartitionLocation(
          queryId,
          decode(pathParts[4]),
          decode(queryParameter(query, "identity")),
          request
      );
    } else if (pathParts.length == 5 && "DELETE".equals(requestMethod(requestBuilder))
               && "partitionReads".equals(pathParts[2])) {
      response = controllerResource.httpEndLiveFramesPartitionRead(
          queryId,
          decode(pathParts[4]),
          decode(queryParameter(query, "identity")),
          request
      );
    } else if (pathParts.length == 3 && "DELETE".equals(requestMethod(requestBuilder))) {
      response = controllerResource.httpReleaseLiveFramesSession(
          decode(pathParts[2]),
          decode(queryParameter(query, "identity")),
          request
      );
    } else {
      return new InternalGatewayResponse(404, Map.of(), new byte[0]);
    }
    return resourceResponse(response);
  }

  private InternalGatewayResponse dispatchWorkerRequest(final String workerTaskId, final String requestPath)
      throws Exception
  {
    final String[] parts = requestPath.split("\\?", 2);
    final String[] pathParts = parts[0].split("/");
    final String queryId = decode(pathParts[2]);
    final int stageNumber = Integer.parseInt(pathParts[3]);
    final int partitionNumber = Integer.parseInt(pathParts[4]);
    final long offset = queryLong(parts.length == 2 ? parts[1] : "", "offset");
    final MSQTestControllerContext controllerContext = productionControllerContext.get();
    if (controllerContext == null) {
      throw new IllegalStateException("Source MSQ controller context is not registered");
    }

    final org.apache.druid.msq.exec.Worker worker = mock(org.apache.druid.msq.exec.Worker.class);
    when(worker.readStageOutput(any(), anyInt(), anyLong())).thenAnswer(invocation -> {
      final org.apache.druid.msq.kernel.StageId stageId = invocation.getArgument(0);
      final int outputPartition = invocation.getArgument(1);
      final long outputOffset = invocation.getArgument(2);
      productionWorkerReads.incrementAndGet();
      return Futures.immediateFuture(new java.io.ByteArrayInputStream(
          controllerContext.readStageOutput(workerTaskId, stageId, outputPartition, outputOffset)
      ));
    });

    final ResourcePermissionMapper permissionMapper = new ResourcePermissionMapper()
    {
      @Override
      public List<ResourceAction> getAdminPermissions()
      {
        return List.of();
      }

      @Override
      public List<ResourceAction> getQueryPermissions(final String queryId)
      {
        return List.of();
      }
    };
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        Map.of(AuthConfig.ALLOW_ALL_NAME, new AllowAllAuthorizer(null))
    );
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(request.getRequestURI()).thenReturn(requestPath);

    final AsyncContext asyncContext = mock(AsyncContext.class);
    final HttpServletResponse httpResponse = mock(HttpServletResponse.class);
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final AtomicInteger status = new AtomicInteger(200);
    final Map<String, String> headers = new HashMap<>();
    final ServletOutputStream servletOutput = new ServletOutputStream()
    {
      @Override
      public boolean isReady()
      {
        return true;
      }

      @Override
      public void setWriteListener(final WriteListener writeListener)
      {
      }

      @Override
      public void write(final int value)
      {
        output.write(value);
      }
    };
    when(asyncContext.getResponse()).thenReturn(httpResponse);
    when(request.startAsync()).thenReturn(asyncContext);
    when(httpResponse.getOutputStream()).thenReturn(servletOutput);
    doAnswer(invocation -> {
      status.set(invocation.getArgument(0));
      return null;
    }).when(httpResponse).setStatus(anyInt());
    doAnswer(invocation -> {
      status.set(invocation.getArgument(0));
      return null;
    }).when(httpResponse).sendError(anyInt());
    doAnswer(invocation -> {
      headers.put(invocation.getArgument(0), invocation.getArgument(1));
      return null;
    }).when(httpResponse).setHeader(anyString(), anyString());

    new WorkerResource(worker, permissionMapper, authorizerMapper)
        .httpGetChannelData(queryId, stageNumber, partitionNumber, offset, request);
    return new InternalGatewayResponse(status.get(), headers, output.toByteArray());
  }

  private InternalGatewayResponse resourceResponse(final Response response) throws IOException
  {
    if (response.getEntity() == null) {
      return new InternalGatewayResponse(response.getStatus(), Map.of(), new byte[0]);
    }
    final byte[] payload = objectMapper.writeValueAsBytes(response.getEntity());
    return new InternalGatewayResponse(response.getStatus(), Map.of("Content-Type", "application/json"), payload);
  }

  private static String requestPath(final RequestBuilder requestBuilder) throws ReflectiveOperationException
  {
    final Field field = RequestBuilder.class.getDeclaredField("encodedPathAndQueryString");
    field.setAccessible(true);
    return (String) field.get(requestBuilder);
  }

  private static String requestMethod(final RequestBuilder requestBuilder) throws ReflectiveOperationException
  {
    final Field field = RequestBuilder.class.getDeclaredField("method");
    field.setAccessible(true);
    return ((HttpMethod) field.get(requestBuilder)).name();
  }

  private static byte[] requestContent(final RequestBuilder requestBuilder) throws ReflectiveOperationException
  {
    final Field field = RequestBuilder.class.getDeclaredField("content");
    field.setAccessible(true);
    return (byte[]) field.get(requestBuilder);
  }

  private static String decode(final String value)
  {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static class InternalGatewayResponse
  {
    private final int status;
    private final Map<String, String> headers;
    private final byte[] body;

    private InternalGatewayResponse(final int status, final Map<String, String> headers, final byte[] body)
    {
      this.status = status;
      this.headers = Map.copyOf(headers);
      this.body = body.clone();
    }
  }

  private final class ProductionGatewayServiceClient implements ServiceClient
  {
    private final String serviceName;

    private ProductionGatewayServiceClient(final String serviceName)
    {
      this.serviceName = serviceName;
    }

    @Override
    public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
        final RequestBuilder requestBuilder,
        final HttpResponseHandler<IntermediateType, FinalType> handler
    )
    {
      try {
        final InternalGatewayResponse gatewayResponse = dispatchInternalRequest(serviceName, requestBuilder);
        final DefaultHttpResponse httpResponse = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(gatewayResponse.status)
        );
        gatewayResponse.headers.forEach(httpResponse.headers()::set);
        ClientResponse<IntermediateType> response = handler.handleResponse(httpResponse, TestHttpClient.NOOP_TRAFFIC_COP);
        if (gatewayResponse.body.length > 0) {
          response = handler.handleChunk(
              response,
              new DefaultHttpContent(Unpooled.wrappedBuffer(gatewayResponse.body)),
              1
          );
        }
        return Futures.immediateFuture(handler.done(response).getObj());
      }
      catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      }
    }

    @Override
    public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
    {
      return this;
    }
  }

  private void respondJson(
      final com.sun.net.httpserver.HttpExchange exchange,
      final int status,
      final Object entity
  ) throws IOException
  {
    final byte[] response = objectMapper.writeValueAsBytes(entity);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, response.length);
    exchange.getResponseBody().write(response);
  }

  private static long queryLong(final String query, final String key)
  {
    for (final String parameter : query.split("&")) {
      final String[] pair = parameter.split("=", 2);
      if (pair[0].equals(key)) {
        return Long.parseLong(pair[1]);
      }
    }
    throw new IllegalArgumentException("Missing query parameter " + key);
  }

  private static String queryParameter(final String query, final String key)
  {
    for (final String parameter : query.split("&")) {
      final String[] pair = parameter.split("=", 2);
      if (pair[0].equals(key)) {
        return pair[1];
      }
    }
    throw new IllegalArgumentException("Missing query parameter " + key);
  }

  @Test
  public void testInsertSelectStarWithFilterAndDiscoveredIntervals()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external() + " WHERE dim1 = 'abc' PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}))
        .verifyResults();
    Assertions.assertFalse(requests.stream().anyMatch(r -> "segmentMetadata".equals(r.path("queryType").asText())));
    Assertions.assertEquals(1, requests.stream().filter(r -> "timeBoundary".equals(r.path("queryType").asText())).count());
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(2, scans.size());
    Assertions.assertTrue(scans.stream().allMatch(r -> r.path("filter").isMissingNode()));
  }

  @Test
  public void testTimePredicateNarrowsRemoteRead()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external()
                + " WHERE __time >= TIMESTAMP '2001-01-03 00:00:00' PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}))
        .verifyResults();
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(1, scans.size());
    Assertions.assertTrue(scans.get(0).path("intervals").get(0).asText().startsWith("2001-01-03T00:00:00.000Z/"));
  }

  @Test
  public void testGroupByWithLocalFilter()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, cnt, SUM(cnt) AS sum_cnt FROM "
                + external() + " WHERE dim1 IN ('2', 'abc') GROUP BY cnt PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, 2L}))
        .verifyResults();
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(2, scans.size());
    Assertions.assertTrue(scans.stream().allMatch(r -> r.path("filter").isMissingNode()));
  }

  @Test
  public void testRemoteMsqBackendExecutesGroupedSelectBeforeTransferringFrames()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, cnt, SUM(cnt) AS sum_cnt FROM "
                + external() + " WHERE dim1 IN ('2', 'abc') GROUP BY cnt HAVING SUM(cnt) > 1 PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, 2L}))
        .verifyResults();

    Assertions.assertNull(remoteSourceError.get());
    Assertions.assertEquals(1, remoteFrameSubmissions.get());
    Assertions.assertEquals(1, releasedSessions.get());
    Assertions.assertEquals(1, remoteSourceSql.size());
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("GROUP BY"));
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("SUM"));
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("HAVING"), remoteSourceSql.get(0));
    Assertions.assertEquals(0, requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).count());
  }

  @Test
  public void testRemoteMsqGroupsAcrossSourceIntervalsWithOneGlobalFilter()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, "
                + "CAST(NULL AS VARCHAR) AS null_value, cnt AS group_key, "
                + "CAST(SUM(cnt) AS BIGINT) AS sum_cnt FROM " + externalAcrossSourceIntervals()
                + " WHERE LENGTH(dim1) > 0 GROUP BY cnt HAVING SUM(cnt) > 1 PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("null_value", ColumnType.STRING)
                                             .add("group_key", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2001-01-03").getMillis(), null, 1L, 5L}
        ))
        .verifyResults();

    Assertions.assertEquals(1, remoteFrameSubmissions.get());
    Assertions.assertEquals(1, remoteSourceSql.size());
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("LENGTH"));
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("GROUP BY"));
    Assertions.assertTrue(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("HAVING"));
    Assertions.assertEquals(
        0,
        requests.stream().filter(r -> "segmentMetadata".equals(r.path("queryType").asText())).count(),
        "Explicit EXTEND schema must not rediscover the full source schema"
    );
  }

  @Test
  public void testRemoteMsqUsesTargetTimezoneWhenSourceDefaultDiffers()
  {
    remoteSourceDefaultTimeZone = "America/Los_Angeles";
    try {
      final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
      queryContext.put("remoteDruidBackend", "msq");
      testIngestQuery()
          .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, "
                  + "CAST(NULL AS VARCHAR) AS null_value, cnt AS group_key, "
                  + "CAST(SUM(cnt) AS BIGINT) AS sum_cnt FROM " + externalAcrossSourceIntervals()
                  + " WHERE LENGTH(dim1) > 0 GROUP BY cnt HAVING SUM(cnt) > 1 PARTITIONED BY DAY")
          .setQueryContext(queryContext)
          .setExpectedDataSource("foo1")
          .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                               .add("null_value", ColumnType.STRING)
                                               .add("group_key", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
          .setExpectedResultRows(List.<Object[]>of(
              new Object[]{DateTimes.of("2001-01-03").getMillis(), null, 1L, 5L}
          ))
          .verifyResults();

      Assertions.assertEquals(1, remoteSourceContexts.size());
      Assertions.assertEquals("UTC", remoteSourceContexts.get(0).get("sqlTimeZone"));
      Assertions.assertTrue(remoteSourceContexts.get(0).get("sqlCurrentTimestamp") instanceof String);
    }
    finally {
      remoteSourceDefaultTimeZone = "UTC";
    }
  }

  @Test
  public void testRemoteMsqAssignsMultipleFramePartitionsOnce()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT __time, dim1, cnt FROM " + external()
                + " WHERE dim1 <> '' PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", 1L},
            new Object[]{DateTimes.of("2000-01-03").getMillis(), "2", 1L},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", 1L},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", 1L},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}
        ))
        .verifyResults();

    Assertions.assertEquals(1, remoteFrameSubmissions.get());
    Assertions.assertEquals(2, remoteFrameFiles.get().size());
    Assertions.assertEquals(2, remotePartitionReadsFromStart.size());
    Assertions.assertEquals(List.of("partition-1", "partition-2"), remotePartitionReadsFromStart.stream().sorted().toList());
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testRemoteMsqKeepsClusterByOnTheTarget()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT __time, dim1, cnt FROM " + external()
                + " WHERE dim1 <> '' PARTITIONED BY DAY CLUSTERED BY dim1")
        .setQueryContext(queryContext)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "10.1", 1L},
            new Object[]{DateTimes.of("2000-01-03").getMillis(), "2", 1L},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), "1", 1L},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), "def", 1L},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}
        ))
        .verifyResults();

    Assertions.assertEquals(1, remoteFrameSubmissions.get());
    Assertions.assertFalse(remoteSourceSql.get(0).toUpperCase(Locale.ROOT).contains("ORDER BY"));
    final Query<?> targetQuery = indexingServiceClient.getMSQControllerTask(TEST_CONTROLLER_TASK_ID)
                                                       .getQuerySpec()
                                                       .getQuery();
    Assertions.assertInstanceOf(ScanQuery.class, targetQuery);
    Assertions.assertEquals(List.of(OrderBy.ascending("dim1")), ((ScanQuery) targetQuery).getOrderBys());
  }

  @Test
  public void testRemoteMsqReleasesAnEmptySourceResult()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external() + " WHERE dim1 = 'no-such-row' PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.of())
        .verifyResults();

    Assertions.assertNull(remoteSourceError.get());
    Assertions.assertEquals(1, remoteFrameSubmissions.get());
    Assertions.assertTrue(remoteFrameFiles.get().isEmpty());
    Assertions.assertEquals(1, releasedSessions.get());
  }

  @Test
  public void testLiveFramesSessionRetainsActualWorkerFrameFileUntilRelease() throws Exception
  {
    final AtomicReference<ControllerImpl> controllerReference = new AtomicReference<>();
    final AtomicReference<MSQTestControllerContext> controllerContextReference = new AtomicReference<>();
    final java.util.concurrent.CountDownLatch controllerRegistered = new java.util.concurrent.CountDownLatch(1);
    indexingServiceClient.setControllerRegistrationHook((controller, controllerContext) -> {
      controllerReference.set(controller);
      controllerContextReference.set(controllerContext);
      controllerRegistered.countDown();
    });

    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("sqlQueryId", "live-frames-retained-output-test");
    final FutureTask<List<Object[]>> queryFuture = new FutureTask<>(() -> {
      final DirectStatement statement = sqlStatementFactory.directStatement(
          SqlQueryPlus.builder()
                      .sql("SELECT __time, cnt FROM foo")
                      .queryContext(queryContext)
                      .auth(CalciteTests.SUPER_USER_AUTH_RESULT)
                      .build()
      );
      try (final LiveFramesRequestContext.Scope ignored = LiveFramesRequestContext.enter()) {
        return statement.execute().getResults().toList();
      }
    });

    final Thread queryThread = new Thread(queryFuture, "live-frames-retained-output-test");
    queryThread.start();
    ControllerImpl controller = null;
    try {
      Assertions.assertTrue(controllerRegistered.await(30, TimeUnit.SECONDS));
      controller = controllerReference.get();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      LiveFramesSession.SessionInfo sessionInfo = controller.getLiveFramesSessionInfo();
      while (!"ready".equals(sessionInfo.getState()) && System.nanoTime() < deadline) {
        Thread.sleep(10);
        sessionInfo = controller.getLiveFramesSessionInfo();
      }

      Assertions.assertEquals("ready", sessionInfo.getState());
      final LiveFramesSession.Manifest manifest = sessionInfo.getManifest();
      Assertions.assertNotNull(manifest);
      Assertions.assertFalse(manifest.getPartitions().isEmpty());

      final LiveFramesSession.Partition partition = manifest.getPartitions().get(0);
      final LiveFramesSession.PartitionLocation location = controller.getLiveFramesPartitionLocation(partition.getId());
      Assertions.assertNotNull(location);
      final byte[] retainedFrameFile = controllerContextReference.get().readStageOutput(
          location.getWorkerId(),
          new StageId(controller.queryId(), manifest.getFinalStageNumber()),
          location.getPartitionNumber(),
          0
      );
      Assertions.assertTrue(retainedFrameFile.length > 0);

      final File frameFile = new File(newTempFolder("retained-live-frame"), "partition.frames");
      Files.write(frameFile.toPath(), retainedFrameFile);
      try (final FrameFile openedFrameFile = FrameFile.open(frameFile, null)) {
        Assertions.assertTrue(openedFrameFile.numFrames() > 0);
      }
    }
    finally {
      if (controller != null) {
        controller.releaseLiveFramesSession();
      } else {
        queryFuture.cancel(true);
      }
      indexingServiceClient.setControllerRegistrationHook(null);
    }

    Assertions.assertFalse(queryFuture.get(30, TimeUnit.SECONDS).isEmpty());
  }

  @Test
  public void testRemoteMsqStreamsActualSourceWorkerFramesThroughProductionGateway() throws Exception
  {
    expectedAuthorization = "Basic cmVhZGVyOnNlY3JldA==";
    final AuthenticationResult sourceAuthentication = new AuthenticationResult(
        "reader",
        "testAuthorizer",
        "testAuthenticator",
        null
    );
    final HttpServletRequest sourceRequest = mock(HttpServletRequest.class);
    when(sourceRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(sourceAuthentication);
    productionRequest.set(sourceRequest);

    final OverlordClient overlordClient = mock(OverlordClient.class);
    when(overlordClient.taskStatus(anyString())).thenAnswer(invocation -> {
      final String taskId = invocation.getArgument(0);
      final ControllerImpl controller = productionController.get();
      final TaskStatusPlus status = controller != null && taskId.equals(controller.queryId())
                                    ? new TaskStatusPlus(
                                        taskId,
                                        null,
                                        "msq_controller",
                                        DateTimes.nowUtc(),
                                        DateTimes.nowUtc(),
                                        TaskState.RUNNING,
                                        null,
                                        1L,
                                        TaskLocation.unknown(),
                                        null,
                                        null
                                    )
                                    : null;
      return Futures.immediateFuture(new TaskStatusResponse(taskId, status));
    });

    final ServiceClientFactory serviceClientFactory = mock(ServiceClientFactory.class);
    when(serviceClientFactory.makeClient(anyString(), any(ServiceLocator.class), any(ServiceRetryPolicy.class)))
        .thenAnswer(invocation -> new ProductionGatewayServiceClient(invocation.getArgument(0)));
    productionResource.set(new RemoteDruidFrameResource(
        sqlStatementFactory,
        objectMapper,
        overlordClient,
        serviceClientFactory,
        () -> Map.of()
    ));
    useProductionGateway.set(true);
    indexingServiceClient.setControllerRegistrationHook((controller, context) -> {
      if (controller.getLiveFramesSessionInfo() != null) {
        productionController.set(controller);
        productionControllerContext.set(context);
        productionControllerRegistered.countDown();
      }
    });

    try {
      final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
      queryContext.put("remoteDruidBackend", "msq");
      testIngestQuery()
          .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, "
                  + "CAST(NULL AS VARCHAR) AS null_value, cnt AS group_key, "
                  + "CAST(SUM(cnt) AS BIGINT) AS sum_cnt FROM " + externalAcrossSourceIntervalsBasic()
                  + " WHERE LENGTH(dim1) > 0 GROUP BY cnt HAVING SUM(cnt) > 1 PARTITIONED BY DAY")
          .setQueryContext(queryContext)
          .setExpectedDataSource("foo1")
          .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                               .add("null_value", ColumnType.STRING)
                                               .add("group_key", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
          .setExpectedResultRows(List.<Object[]>of(
              new Object[]{DateTimes.of("2001-01-03").getMillis(), null, 1L, 5L}
          ))
          .verifyResults();

      Assertions.assertFalse(productionSourceSql.isEmpty());
      Assertions.assertTrue(productionSourceSql.get(0).toUpperCase(Locale.ROOT).contains("GROUP BY"));
      Assertions.assertTrue(productionSourceSql.get(0).toUpperCase(Locale.ROOT).contains("HAVING"));
      Assertions.assertTrue(productionSourceSql.get(0).toUpperCase(Locale.ROOT).contains("LENGTH"));
      Assertions.assertFalse(productionSourceSql.get(0).toLowerCase(Locale.ROOT).contains("password"));
      Assertions.assertFalse(productionSourceSql.get(0).contains("secret"));
      Assertions.assertTrue(productionGatewayPaths.stream().anyMatch(
          path -> path.startsWith("GET ") && path.contains("/sql/remote-frames/") && !path.contains("/partitions/")
      ));
      Assertions.assertTrue(productionGatewayPaths.stream().anyMatch(
          path -> path.startsWith("GET ") && path.contains("/partitions/")
      ));
      Assertions.assertTrue(productionGatewayPaths.stream().anyMatch(path -> path.startsWith("DELETE ")));
      Assertions.assertTrue(productionWorkerReads.get() > 0);
      Assertions.assertEquals(1, releasedSessions.get());

      final FutureTask<Response> submission = productionSubmitFuture.get();
      Assertions.assertNotNull(submission);
      final Response submitResponse = submission.get(30, TimeUnit.SECONDS);
      Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), submitResponse.getStatus());
      Assertions.assertEquals(
          productionController.get().queryId(),
          ((LiveFramesSession.SessionInfo) submitResponse.getEntity()).getQueryId()
      );
    }
    finally {
      final ControllerImpl controller = productionController.get();
      if (controller != null) {
        controller.releaseLiveFramesSession();
      }
      indexingServiceClient.setControllerRegistrationHook(null);
      useProductionGateway.set(false);
    }
  }

  @Test
  public void testRejectsUnsupportedOrderByBeforeSubmittingRemoteSourceQuery()
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external() + " ORDER BY cnt PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedValidationErrorMatcher(exception -> Assertions.assertTrue(
            exception.getMessage().contains("ORDER BY clause"),
            exception.toString()
        ))
        .verifyPlanningErrors();

    Assertions.assertEquals(0, remoteFrameSubmissions.get());
  }

  @Test
  public void testRejectsUnsupportedRemoteMsqShapesBeforeSubmittingSourceQueries()
  {
    assertRemoteMsqPlanningRejected(
        "SELECT r.__time, r.dim1, r.cnt FROM " + external() + " r JOIN foo l ON r.__time = l.__time"
    );
    assertRemoteMsqPlanningRejected(
        "SELECT __time, dim1, cnt FROM " + external() + " UNION ALL SELECT __time, dim1, cnt FROM " + external()
    );
    assertRemoteMsqPlanningRejected(
        "SELECT r.__time, r.dim1, r.cnt FROM " + external() + " r JOIN " + external() + " s ON r.__time = s.__time"
    );
    assertRemoteMsqPlanningRejected(
        "SELECT __time, dim1, cnt FROM " + external() + " UNION ALL SELECT __time, dim1, cnt FROM foo"
    );
    assertRemoteMsqPlanningRejected(
        "SELECT __time, ROW_NUMBER() OVER (ORDER BY cnt) AS row_num FROM " + external()
    );
  }

  private void assertRemoteMsqPlanningRejected(final String selectSql)
  {
    final Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put("remoteDruidBackend", "msq");
    testIngestQuery()
        .setSql("INSERT INTO foo1 " + selectSql + " PARTITIONED BY DAY")
        .setQueryContext(queryContext)
        .setExpectedValidationErrorMatcher(exception -> Assertions.assertTrue(
            exception.getMessage() != null && !exception.getMessage().isBlank(),
            exception.toString()
        ))
        .verifyPlanningErrors();

    Assertions.assertEquals(0, remoteFrameSubmissions.get(), "Unsupported source shapes must fail before submission");
  }

  @Test
  public void testBasicAuthenticationAndDiscoveredSchemaWithBoundPassword()
  {
    expectedAuthorization = "Basic cmVhZGVyOnNlY3JldA==";
    final String remote = "TABLE(DRUID(endpoint => 'http://localhost:" + server.getAddress().getPort()
                          + "/druid/v2/', dataSource => 'foo', authType => 'basic', username => 'reader', password => ?,"
                          + " splitDurationMillis => 31536000000))";
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + remote + " WHERE dim1 = 'abc' PARTITIONED BY DAY")
        .setDynamicParameters(List.of(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "secret")))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("dim1", ColumnType.STRING).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc"}))
        .verifyResults();
    Assertions.assertTrue(requests.stream().anyMatch(r -> "segmentMetadata".equals(r.path("queryType").asText())));
    Assertions.assertEquals(2, requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).count());
  }

}
