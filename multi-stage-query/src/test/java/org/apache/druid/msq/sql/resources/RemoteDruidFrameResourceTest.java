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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.impl.RemoteDruidFrameInputSource;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler.TrafficCop;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/// Validates the source remote-frame submission boundary before any MSQ task is created.
public class RemoteDruidFrameResourceTest
{
  private SqlStatementFactory statementFactory;
  private OverlordClient overlordClient;
  private ServiceClientFactory serviceClientFactory;
  private RemoteDruidFrameResource resource;
  private HttpServletRequest request;

  @BeforeEach
  public void setUp()
  {
    statementFactory = mock(SqlStatementFactory.class);
    final ObjectMapper mapper = new DefaultObjectMapper();
    overlordClient = mock(OverlordClient.class);
    serviceClientFactory = mock(ServiceClientFactory.class);
    resource = new RemoteDruidFrameResource(
        statementFactory,
        mapper,
        overlordClient,
        serviceClientFactory,
        mock(QueryConfigProvider.class)
    );
    request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(
        new AuthenticationResult("reader", "testAuthorizer", "testAuthenticator", null)
    );
  }

  @Test
  public void testRejectsNonSelectAndMultipleStatementsBeforeSubmission()
  {
    final Response insertResponse = resource.submit(query("INSERT INTO target SELECT 1"), request);
    final Response multipleStatementsResponse = resource.submit(query("SELECT 1; SELECT 2"), request);

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), insertResponse.getStatus());
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), multipleStatementsResponse.getStatus());
    verify(statementFactory, never()).httpStatement(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testRejectsRemoteDruidRecursionBeforeSubmission()
  {
    final Response response = resource.submit(
        query("SELECT * FROM TABLE(DRUID(endpoint => 'http://source', dataSource => 'events'))"),
        request
    );

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    verify(statementFactory, never()).httpStatement(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testRejectsUnsupportedProtocolVersionBeforeParsingSql()
  {
    final RemoteDruidFrameResource.RemoteFramesQueryRequest invalidVersion = new RemoteDruidFrameResource.RemoteFramesQueryRequest(
        "INSERT INTO target SELECT 1",
        "client-request",
        RemoteDruidFrameInputSource.PROTOCOL_VERSION + 1,
        Map.of()
    );

    final Response response = resource.submit(invalidVersion, request);

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(statementFactory, never()).httpStatement(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testReleaseIsIdempotentAfterSourceTaskCompletion()
  {
    final String queryId = "completed-query";
    final var now = DateTimes.nowUtc();
    final TaskStatusPlus completedStatus = new TaskStatusPlus(
        queryId,
        null,
        "msq_controller",
        now,
        now,
        TaskState.SUCCESS,
        null,
        1L,
        TaskLocation.unknown(),
        null,
        null
    );
    when(overlordClient.taskStatus(queryId)).thenReturn(
        Futures.immediateFuture(new TaskStatusResponse(queryId, completedStatus))
    );

    final Response response = resource.release(queryId, request);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verifyNoInteractions(serviceClientFactory);
  }

  @Test
  public void testTaskRequestDeadlineCancelsInFlightRequest()
  {
    final SettableFuture<String> pendingRequest = SettableFuture.create();

    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> RemoteDruidFrameResource.awaitTaskRequest(pendingRequest, 1, TimeUnit.MILLISECONDS)
    );

    Assertions.assertTrue(pendingRequest.isCancelled());
    Assertions.assertTrue(exception.getMessage().contains("deadline"));
  }

  @Test
  public void testFrameChunkRequestRemainsPendingUntilBodyCompletes()
  {
    final RemoteDruidFrameResource.CompleteInputStreamFullResponseHandler responseHandler =
        new RemoteDruidFrameResource.CompleteInputStreamFullResponseHandler();
    final TrafficCop trafficCop = mock(TrafficCop.class);
    final ClientResponse<InputStreamFullResponseHolder> response = responseHandler.handleResponse(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
        trafficCop
    );

    Assertions.assertFalse(response.isFinished());
    Assertions.assertTrue(responseHandler.done(response).isFinished());
  }

  private static RemoteDruidFrameResource.RemoteFramesQueryRequest query(final String sql)
  {
    return new RemoteDruidFrameResource.RemoteFramesQueryRequest(
        sql,
        "client-request",
        RemoteDruidFrameInputSource.PROTOCOL_VERSION,
        Map.of()
    );
  }
}
