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

package org.apache.druid.msq.sql.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.impl.RemoteDruidFrameInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.msq.exec.LiveFramesSession;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.rpc.BaseWorkerClientImpl;
import org.apache.druid.msq.sql.LiveFramesRequestContext;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.druid.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.parser.StatementAndSetContext;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/// Authenticated source Broker API for submitting SQL and streaming retained MSQ result frames.
@Path("/druid/v2/sql/remote-frames")
public class RemoteDruidFrameResource
{
  public static final String LAST_FETCH_HEADER = "X-Druid-Frame-Last-Fetch";
  private static final Logger LOG = new Logger(RemoteDruidFrameResource.class);
  private static final long DEFAULT_SOURCE_QUERY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(25);
  private static final long TASK_RPC_DEADLINE_MILLIS = TimeUnit.SECONDS.toMillis(25);
  private static final long TASK_RPC_ATTEMPT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(20);
  private static final StandardRetryPolicy TASK_RPC_RETRY_POLICY = StandardRetryPolicy.builder()
      .maxAttempts(4)
      .maxWaitMillis(TimeUnit.SECONDS.toMillis(1))
      .retryLoggable(false)
      .build();
  private static final Set<String> ALLOWED_CONTEXT_KEYS = Set.of(
      "sqlTimeZone",
      "sqlCurrentTimestamp",
      "sqlStringifyArrays",
      "sqlUseApproximateCountDistinct",
      "sqlUseFallback",
      "timeout",
      "priority"
  );

  private final SqlStatementFactory statementFactory;
  private final ObjectMapper mapper;
  private final OverlordClient overlordClient;
  private final ServiceClientFactory serviceClientFactory;
  private final QueryConfigProvider queryConfigProvider;

  @Inject
  public RemoteDruidFrameResource(
      @MultiStageQuery final SqlStatementFactory statementFactory,
      final ObjectMapper mapper,
      final OverlordClient overlordClient,
      @EscalatedGlobal final ServiceClientFactory serviceClientFactory,
      final QueryConfigProvider queryConfigProvider
  )
  {
    this.statementFactory = statementFactory;
    this.mapper = mapper;
    this.overlordClient = overlordClient;
    this.serviceClientFactory = serviceClientFactory;
    this.queryConfigProvider = queryConfigProvider;
  }

  /// Submits one authorized SELECT to a source MSQ controller and returns its session identifier.
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response submit(final RemoteFramesQueryRequest request, @Context final HttpServletRequest httpRequest)
  {
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(httpRequest);
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(httpRequest);
    if (request == null || request.sql == null || request.sql.length() > 1_000_000
        || request.clientRequestId == null || request.clientRequestId.isBlank() || request.clientRequestId.length() > 128) {
      return error(Response.Status.BAD_REQUEST, "Invalid remote frame query request");
    }
    if (request.protocolVersion != RemoteDruidFrameInputSource.PROTOCOL_VERSION) {
      return error(Response.Status.CONFLICT, "The requested remote frame protocol version is not supported");
    }
    @Nullable String taskIdForDeduplication = null;
    try {
      validateSql(request.sql);
      final Map<String, Object> queryContext = validatedSourceContext(request.context);
      queryContext.putIfAbsent(QueryContexts.TIMEOUT_KEY, DEFAULT_SOURCE_QUERY_TIMEOUT_MILLIS);
      final String sqlQueryId = requestId(
          authenticationResult.getIdentity(),
          request.clientRequestId,
          request.sql,
          queryContext
      );
      taskIdForDeduplication = MSQTasks.controllerTaskId(sqlQueryId);
      final Response existing = existingTaskResponse(taskIdForDeduplication);
      if (existing != null) {
        return existing;
      }
      queryContext.put(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId);
      queryContext.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, "liveFrames");

      final SqlQuery sqlQuery = new SqlQuery(
          request.sql,
          ResultFormat.OBJECT,
          false,
          false,
          false,
          queryContext,
          null
      );
      final SqlQueryPlus sqlQueryPlus = SqlResource.makeSqlQueryPlus(
          sqlQuery,
          httpRequest,
          queryConfigProvider.getContext()
      );
      final HttpStatement statement = statementFactory.httpStatement(sqlQueryPlus, httpRequest);
      try {
        final String taskId;
        try (final LiveFramesRequestContext.Scope ignored = LiveFramesRequestContext.enter()) {
          final DirectStatement.ResultSet plan = statement.plan();
          taskId = taskIdFrom(plan.run());
        }
        return Response.status(Response.Status.ACCEPTED)
                       .entity(new LiveFramesSession.SessionInfo(taskId, "submitted", null, null, 0))
                       .build();
      }
      finally {
        statement.close();
      }
    }
    catch (DruidException e) {
      final Response existing = existingSubmissionAfterFailure(taskIdForDeduplication);
      if (existing != null) {
        return existing;
      }
      return error(Response.Status.BAD_REQUEST, e.getMessage());
    }
    catch (ForbiddenException e) {
      return error(Response.Status.FORBIDDEN, "Remote frame query authorization failed");
    }
    catch (Exception e) {
      final Response existing = existingSubmissionAfterFailure(taskIdForDeduplication);
      if (existing != null) {
        return existing;
      }
      LOG.noStackTrace().warn(e, "Remote frame query submission failed for request[%s]", request.clientRequestId);
      return error(Response.Status.INTERNAL_SERVER_ERROR, "Remote frame query submission failed on the source cluster");
    }
  }

  @Nullable
  private Response existingSubmissionAfterFailure(@Nullable final String taskId)
  {
    if (taskId == null) {
      return null;
    }
    try {
      return existingTaskResponse(taskId);
    }
    catch (RuntimeException e) {
      return null;
    }
  }

  @Nullable
  private Response existingTaskResponse(final String taskId)
  {
    final TaskStatusResponse taskStatusResponse = FutureUtils.getUnchecked(overlordClient.taskStatus(taskId), true);
    if (taskStatusResponse.getStatus() == null) {
      return null;
    }
    if (taskStatusResponse.getStatus().getStatusCode() != null
        && taskStatusResponse.getStatus().getStatusCode().isComplete()) {
      return error(Response.Status.CONFLICT, "This remote frame request was already completed; submit it with a new request ID");
    }
    return Response.status(Response.Status.ACCEPTED)
                   .entity(new LiveFramesSession.SessionInfo(taskId, "submitted", null, null, 0))
                   .build();
  }

  /// Returns state and the immutable manifest for an owner-authenticated source session.
  @GET
  @Path("/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus(
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest request
  )
  {
    final String identity = authenticatedIdentity(request);
    try (final TaskClient taskClient = taskClient(queryId)) {
      final BytesFullResponseHolder response = taskClient.json(
          HttpMethod.GET,
          "/remoteFrames/status/" + encode(queryId) + "?identity=" + encode(identity),
          null
      );
      return fromJsonResponse(response, LiveFramesSession.SessionInfo.class);
    }
    catch (Exception e) {
      return internalError(e);
    }
  }

  /// Renews a session lease for a bounded duration using the authenticated owner identity.
  @POST
  @Path("/{queryId}/lease")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response renewLease(
      final LeaseRequest leaseRequest,
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest request
  )
  {
    if (leaseRequest == null || leaseRequest.leaseMillis <= 0
        || leaseRequest.leaseMillis > LiveFramesSession.MAX_LEASE_MILLIS) {
      return error(Response.Status.BAD_REQUEST, "Remote frame lease duration is outside the supported range");
    }
    final String identity = authenticatedIdentity(request);
    try (final TaskClient taskClient = taskClient(queryId)) {
      final BytesFullResponseHolder response = taskClient.json(
          HttpMethod.POST,
          "/remoteFrames/lease/" + encode(queryId),
          mapper.writeValueAsBytes(Map.of("identity", identity, "leaseMillis", leaseRequest.leaseMillis))
      );
      return fromJsonResponse(response, Boolean.class);
    }
    catch (Exception e) {
      return internalError(e);
    }
  }

  /// Releases a source session and allows its controller to clean retained worker files.
  @DELETE
  @Path("/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response release(
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest request
  )
  {
    final String identity = authenticatedIdentity(request);
    if (isTaskCompleteOrMissing(queryId)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    try (final TaskClient taskClient = taskClient(queryId)) {
      final BytesFullResponseHolder response = taskClient.json(
          HttpMethod.DELETE,
          "/remoteFrames/" + encode(queryId) + "?identity=" + encode(identity),
          null
      );
      if (response.getStatus().code() == 404) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      if (response.getStatus().code() != 202) {
        return error(502, "Source controller did not accept session release");
      }
      return Response.status(Response.Status.ACCEPTED).build();
    }
    catch (Exception e) {
      return isTaskCompleteOrMissing(queryId)
             ? Response.status(Response.Status.NOT_FOUND).build()
             : internalError(e);
    }
  }

  private boolean isTaskCompleteOrMissing(final String queryId)
  {
    try {
      final TaskStatusResponse taskStatus = FutureUtils.getUnchecked(overlordClient.taskStatus(queryId), true);
      return taskStatus.getStatus() == null
             || taskStatus.getStatus().getStatusCode() != null && taskStatus.getStatus().getStatusCode().isComplete();
    }
    catch (RuntimeException e) {
      return false;
    }
  }

  /// Streams one raw source FrameFile chunk while resolving worker routes only on the source cluster.
  @GET
  @Path("/{queryId}/partitions/{partitionId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response readPartition(
      @PathParam("queryId") final String queryId,
      @PathParam("partitionId") final String partitionId,
      @QueryParam("attemptId") @Nullable final String attemptId,
      @QueryParam("offset") final long offset,
      @Context final HttpServletRequest request
  )
  {
    if (offset < 0 || attemptId == null || attemptId.isBlank()) {
      return error(Response.Status.BAD_REQUEST, "Invalid remote frame partition request");
    }
    final String identity = authenticatedIdentity(request);
    String readId = null;
    boolean streamingResponse = false;
    try {
      final LiveFramesSession.PartitionLocation location;
      try (final TaskClient taskClient = taskClient(queryId)) {
        final BytesFullResponseHolder partition = taskClient.json(
            HttpMethod.GET,
            "/remoteFrames/partitions/" + encode(queryId) + "/" + encode(partitionId)
            + "?identity=" + encode(identity),
            null
        );
        if (partition.getStatus().code() == 404) {
          return Response.status(Response.Status.NOT_FOUND).build();
        }
        location = mapper.readValue(partition.getContent(), LiveFramesSession.PartitionLocation.class);
      }

      readId = location.getReadId();
      if (readId == null || location.getReadExpiresAtMillis() <= 0) {
        return error(502, "Source controller did not create a bounded partition read");
      }
      if (!attemptId.equals(location.getAttemptId()) || location.getFinalStageNumber() < 0) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      final InputStreamFullResponseHolder chunk;
      try (final TaskClient workerClient = taskClient(location.getWorkerId())) {
        chunk = workerClient.frameChunk(
            queryId,
            location.getFinalStageNumber(),
            location.getPartitionNumber(),
            offset,
            location.getReadExpiresAtMillis()
        );
      }
      if (chunk.getStatus().code() == 404) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      if (chunk.getStatus().code() != 200) {
        return error(502, "Source worker could not read the retained frame partition");
      }
      final String lastFetch = chunk.getResponse().headers().get("X-Druid-Frame-Last-Fetch");
      final String partitionReadId = readId;
      final long readExpiresAtMillis = location.getReadExpiresAtMillis();
      streamingResponse = true;
      return Response.ok((StreamingOutput) output -> {
        try {
          copyChunk(chunk.getContent(), output, readExpiresAtMillis);
        }
        finally {
          endPartitionRead(queryId, partitionReadId, identity);
        }
      })
                     .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
                     .header("X-Druid-Frame-Last-Fetch", lastFetch == null ? "false" : lastFetch)
                     .build();
    }
    catch (Exception e) {
      return internalError(e);
    }
    finally {
      if (!streamingResponse && readId != null) {
        endPartitionRead(queryId, readId, identity);
      }
    }
  }

  private String authenticatedIdentity(final HttpServletRequest request)
  {
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    return AuthorizationUtils.authenticationResultFromRequest(request).getIdentity();
  }

  private TaskClient taskClient(final String taskId)
  {
    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(taskId, overlordClient);
    final ServiceClient client = serviceClientFactory.makeClient(
        taskId,
        locator,
        new SpecificTaskRetryPolicy(taskId, TASK_RPC_RETRY_POLICY)
    );
    return new TaskClient(locator, client);
  }

  private Response fromJsonResponse(final BytesFullResponseHolder response, final Class<?> responseClass) throws IOException
  {
    if (response.getStatus().code() == 404) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    if (response.getStatus().code() < 200 || response.getStatus().code() >= 300) {
      return error(502, "Source controller request failed");
    }
    if (responseClass == Boolean.class && response.getContent().length == 0) {
      return Response.ok(Boolean.TRUE).build();
    }
    return Response.ok(mapper.readValue(response.getContent(), responseClass)).build();
  }

  private Response internalError(final Exception exception)
  {
    LOG.noStackTrace().warn(exception, "Remote frame source gateway request failed");
    return error(502, "Remote frame source controller or worker request failed");
  }

  private static Response error(final Response.Status status, final String message)
  {
    return Response.status(status).entity(Map.of("error", message)).type(MediaType.APPLICATION_JSON_TYPE).build();
  }

  private static Response error(final int status, final String message)
  {
    return Response.status(status).entity(Map.of("error", message)).type(MediaType.APPLICATION_JSON_TYPE).build();
  }

  private void endPartitionRead(final String queryId, final String readId, final String identity)
  {
    try (final TaskClient taskClient = taskClient(queryId)) {
      final BytesFullResponseHolder response = taskClient.json(
          HttpMethod.DELETE,
          "/remoteFrames/partitionReads/" + encode(queryId) + "/" + encode(readId)
          + "?identity=" + encode(identity),
          null
      );
      if (response.getStatus().code() != 202 && response.getStatus().code() != 404) {
        LOG.noStackTrace().warn("Source controller did not close a bounded remote frame partition read");
      }
    }
    catch (Exception e) {
      LOG.noStackTrace().warn(e, "Could not close a bounded remote frame partition read");
    }
  }

  private static void copyChunk(
      final InputStream input,
      final OutputStream output,
      final long readExpiresAtMillis
  ) throws IOException
  {
    try (input) {
      final byte[] buffer = new byte[8192];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        if (System.currentTimeMillis() >= readExpiresAtMillis) {
          throw new InterruptedIOException("Remote frame partition read exceeded its source lease");
        }
        if (count > 0) {
          output.write(buffer, 0, count);
        }
      }
    }
  }

  static <T> T awaitTaskRequest(
      final ListenableFuture<T> future,
      final long timeout,
      final TimeUnit timeoutUnit
  ) throws IOException
  {
    try {
      return future.get(timeout, timeoutUnit);
    }
    catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      final InterruptedIOException interrupted = new InterruptedIOException("Remote frame task request was interrupted");
      interrupted.initCause(e);
      throw interrupted;
    }
    catch (TimeoutException e) {
      future.cancel(true);
      throw new IOException("Remote frame task request exceeded its deadline", e);
    }
    catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Remote frame task request failed", cause);
    }
  }

  private static String requestId(
      final String identity,
      final String requestId,
      final String sql,
      final Map<String, Object> context
  )
  {
    return Hashing.sha256()
                 .hashString(identity + "\u0000" + requestId + "\u0000" + sql + "\u0000" + new java.util.TreeMap<>(context),
                             StandardCharsets.UTF_8)
                 .toString();
  }

  private static Map<String, Object> validatedSourceContext(@Nullable final Map<String, Object> context)
  {
    final Map<String, Object> result = new HashMap<>();
    if (context == null) {
      return result;
    }
    if (context.size() > 32) {
      throw InvalidInput.exception("Remote frame source context has too many entries");
    }
    for (final Map.Entry<String, Object> entry : context.entrySet()) {
      if (!ALLOWED_CONTEXT_KEYS.contains(entry.getKey())) {
        throw InvalidInput.exception("Remote frame source context key[%s] is not supported", entry.getKey());
      }
      final Object value = entry.getValue();
      switch (entry.getKey()) {
        case "sqlTimeZone":
        case "sqlCurrentTimestamp":
          if (!(value instanceof String) || ((String) value).length() > 128) {
            throw InvalidInput.exception("Remote frame source context value for key[%s] is invalid", entry.getKey());
          }
          break;
        case "sqlStringifyArrays":
        case "sqlUseApproximateCountDistinct":
        case "sqlUseFallback":
          if (!(value instanceof Boolean)
              && (!(value instanceof String) || !("true".equals(value) || "false".equals(value)))) {
            throw InvalidInput.exception("Remote frame source context value for key[%s] is invalid", entry.getKey());
          }
          break;
        case "timeout":
        case "priority":
          final long minimum = "priority".equals(entry.getKey()) ? Integer.MIN_VALUE : 1;
          final long maximum = "timeout".equals(entry.getKey()) ? 86_400_000L : Integer.MAX_VALUE;
          if (!isBoundedInteger(value, minimum, maximum)) {
            throw InvalidInput.exception("Remote frame source context value for key[%s] is invalid", entry.getKey());
          }
          break;
        default:
          throw InvalidInput.exception("Remote frame source context key[%s] is not supported", entry.getKey());
      }
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  private static boolean isBoundedInteger(final Object value, final long minimum, final long maximum)
  {
    try {
      final long parsed;
      if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
        parsed = ((Number) value).longValue();
      } else if (value instanceof String) {
        parsed = Long.parseLong((String) value);
      } else {
        return false;
      }
      return parsed >= minimum && parsed <= maximum;
    }
    catch (RuntimeException e) {
      return false;
    }
  }

  private static void validateSql(final String sql)
  {
    final StatementAndSetContext parsed = DruidSqlParser.parse(sql, false);
    final SqlNode statement = parsed.getMainStatement();
    if (!(statement instanceof SqlSelect)) {
      throw InvalidSqlInput.exception("Remote frame API accepts a single SELECT statement");
    }
    final boolean[] containsRemoteFunction = {false};
    statement.accept(new SqlBasicVisitor<Void>()
    {
      @Override
      public Void visit(final SqlCall call)
      {
        if ("DRUID".equalsIgnoreCase(call.getOperator().getName())) {
          containsRemoteFunction[0] = true;
        }
        for (final SqlNode operand : call.getOperandList()) {
          if (operand != null) {
            operand.accept(this);
          }
        }
        return null;
      }
    });
    if (containsRemoteFunction[0]) {
      throw InvalidSqlInput.exception("Remote frame source SQL cannot call the remote DRUID table function");
    }
  }

  private String taskIdFrom(final QueryResponse<Object[]> response) throws IOException
  {
    try (final Yielder<Object[]> yielder = Yielders.each(response.getResults())) {
      if (yielder.isDone() || yielder.get().length != 1 || !(yielder.get()[0] instanceof String taskId)) {
        throw new IllegalStateException("Remote frame MSQ submission did not return a controller task ID");
      }
      return taskId;
    }
  }

  private static String encode(final String value)
  {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  /// Request body for an authenticated source-side SELECT submission.
  public static class RemoteFramesQueryRequest
  {
    private final String sql;
    private final String clientRequestId;
    private final int protocolVersion;
    private final Map<String, Object> context;

    @JsonCreator
    public RemoteFramesQueryRequest(
        @JsonProperty("sql") final String sql,
        @JsonProperty("clientRequestId") final String clientRequestId,
        @JsonProperty("protocolVersion") final int protocolVersion,
        @JsonProperty("context") @Nullable final Map<String, Object> context
    )
    {
      this.sql = sql;
      this.clientRequestId = clientRequestId;
      this.protocolVersion = protocolVersion;
      this.context = context == null ? Map.of() : Map.copyOf(context);
    }

    @JsonProperty
    public String getSql()
    {
      return sql;
    }

    @JsonProperty
    public String getClientRequestId()
    {
      return clientRequestId;
    }

    @JsonProperty
    public int getProtocolVersion()
    {
      return protocolVersion;
    }

    @JsonProperty
    public Map<String, Object> getContext()
    {
      return context;
    }
  }

  /// Request body for a bounded lease extension.
  public static class LeaseRequest
  {
    private final long leaseMillis;

    @JsonCreator
    public LeaseRequest(@JsonProperty("leaseMillis") final long leaseMillis)
    {
      this.leaseMillis = leaseMillis;
    }

    @JsonProperty
    public long getLeaseMillis()
    {
      return leaseMillis;
    }
  }

  private static class TaskClient implements AutoCloseable
  {
    private final SpecificTaskServiceLocator locator;
    private final ServiceClient serviceClient;

    private TaskClient(final SpecificTaskServiceLocator locator, final ServiceClient serviceClient)
    {
      this.locator = locator;
      this.serviceClient = serviceClient;
    }

    private BytesFullResponseHolder json(final HttpMethod method, final String path, @Nullable final byte[] body)
        throws IOException
    {
      try {
        final RequestBuilder builder = new RequestBuilder(method, path);
        builder.timeout(Duration.millis(TASK_RPC_ATTEMPT_TIMEOUT_MILLIS));
        if (body != null) {
          builder.content(MediaType.APPLICATION_JSON, body);
        }
        return RemoteDruidFrameResource.awaitTaskRequest(
            serviceClient.asyncRequest(builder, new BytesFullResponseHandler()),
            TASK_RPC_DEADLINE_MILLIS,
            TimeUnit.MILLISECONDS
        );
      }
      catch (RuntimeException e) {
        throw new IOException("Source controller request failed", e);
      }
    }

    private InputStreamFullResponseHolder frameChunk(
        final String queryId,
        final int stageNumber,
        final int partitionNumber,
        final long offset,
        final long readExpiresAtMillis
    ) throws IOException
    {
      final long remainingMillis = readExpiresAtMillis - System.currentTimeMillis();
      if (remainingMillis <= 0) {
        throw new IOException("Remote frame partition read lease expired before the worker request");
      }
      final String path = BaseWorkerClientImpl.getStagePartitionPath(
          new org.apache.druid.msq.kernel.StageId(queryId, stageNumber),
          partitionNumber
      ) + "?offset=" + offset;
      try {
        final RequestBuilder builder = new RequestBuilder(HttpMethod.GET, path)
            .header("Accept-Encoding", "identity")
            .timeout(Duration.millis(Math.min(TASK_RPC_ATTEMPT_TIMEOUT_MILLIS, remainingMillis)));
        return RemoteDruidFrameResource.awaitTaskRequest(
            serviceClient.asyncRequest(builder, new CompleteInputStreamFullResponseHandler()),
            Math.min(TASK_RPC_DEADLINE_MILLIS, remainingMillis),
            TimeUnit.MILLISECONDS
        );
      }
      catch (RuntimeException e) {
        throw new IOException("Source worker frame request failed", e);
      }
    }

    @Override
    public void close() throws IOException
    {
      locator.close();
    }
  }

  /// Keeps the task RPC future pending until the bounded worker chunk body has been received.
  static final class CompleteInputStreamFullResponseHandler extends InputStreamFullResponseHandler
  {
    @Override
    public ClientResponse<InputStreamFullResponseHolder> handleResponse(
        final HttpResponse response,
        final TrafficCop trafficCop
    )
    {
      return ClientResponse.unfinished(new InputStreamFullResponseHolder(response));
    }
  }
}
