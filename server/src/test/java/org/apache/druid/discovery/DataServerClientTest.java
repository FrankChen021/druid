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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.client.TestHttpClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.QueryResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.druid.query.Druids.newScanQueryBuilder;
import static org.mockito.Mockito.mock;

public class DataServerClientTest
{
  private static final SegmentDescriptor SEGMENT_1 = new SegmentDescriptor(Intervals.of("2003/2004"), "v0", 1);
  private static final ServiceLocation LOCAL_SERVICE_LOCATION = new ServiceLocation("localhost", 8083, -1, "");
  private MockServiceClient serviceClient;
  private ObjectMapper jsonMapper;
  private ScanQuery query;
  private DataServerClient target;

  @BeforeEach
  public void setUp()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    serviceClient = new MockServiceClient();
    ServiceClientFactory serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> serviceClient;

    query = newScanQueryBuilder()
      .dataSource("dataSource1")
      .intervals(new MultipleSpecificSegmentSpec(ImmutableList.of(SEGMENT_1)))
      .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
      .context(ImmutableMap.of("defaultTimeout", 5000L))
      .build();

    target = new DataServerClient(
        serviceClientFactory,
        mock(ServiceLocation.class),
        jsonMapper,
        StandardRetryPolicy.noRetries()
    );
  }

  @Test
  public void testFetchSegmentFromDataServer() throws JsonProcessingException, ExecutionException, InterruptedException
  {
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of("id", "name"),
        ImmutableList.of(
            ImmutableList.of(1, "abc"),
            ImmutableList.of(5, "efg")
        ));

    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/druid/v2/")
        .jsonContent(jsonMapper, query);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(scanResultValue))
    );

    ResponseContext responseContext = DefaultResponseContext.createEmpty();
    Sequence<ScanResultValue> result = target.run(
        query,
        responseContext,
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        Closer.create()
    ).get();

    Assertions.assertEquals(ImmutableList.of(scanResultValue), result.toList());
  }

  @Test
  public void testMissingSegmentsHeaderShouldAccumulate() throws JsonProcessingException
  {
    DataServerResponse dataServerResponse = new DataServerResponse(ImmutableList.of(SEGMENT_1));
    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/druid/v2/")
        .jsonContent(jsonMapper, query);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON, QueryResource.HEADER_RESPONSE_CONTEXT, jsonMapper.writeValueAsString(dataServerResponse)),
        jsonMapper.writeValueAsBytes(null)
    );

    ResponseContext responseContext = new DefaultResponseContext();
    target.run(
        query,
        responseContext,
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        Closer.create()
    );

    Assertions.assertEquals(1, responseContext.getMissingSegments().size());
  }

  @Test
  public void testQueryFailure() throws JsonProcessingException
  {
    ScanQuery scanQueryWithTimeout = query.withOverriddenContext(ImmutableMap.of("maxQueuedBytes", 1, "timeout", 0));
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of("id", "name"),
        ImmutableList.of(
            ImmutableList.of(1, "abc"),
            ImmutableList.of(5, "efg")
        ));

    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/druid/v2/")
        .jsonContent(jsonMapper, scanQueryWithTimeout);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(scanResultValue))
    );

    ResponseContext responseContext = new DefaultResponseContext();
    Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> target.run(
            scanQueryWithTimeout,
            responseContext,
            jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
            Closer.create()
        ).get().toList()
    );
  }

  /** Node-local requests and their cancellation are both sent to the node-local {@code /druid/v2} endpoint. */
  @Test
  public void testNodeLocalRequestAndCancellationCarryRoutingHeader() throws Exception
  {
    final List<RequestBuilder> requests = runNodeLocalQueryAndClose(false);

    Assertions.assertEquals(2, requests.size());
    final Request queryRequest = requests.get(0).build(LOCAL_SERVICE_LOCATION);
    Assertions.assertEquals(HttpMethod.POST, queryRequest.getMethod());
    Assertions.assertTrue(
        queryRequest.getHeaders().get(QueryResource.HEADER_NATIVE_QUERY_ROUTE)
                    .contains(QueryResource.NATIVE_QUERY_ROUTE_LOCAL)
    );
    final Request cancelRequest = requests.get(1).build(LOCAL_SERVICE_LOCATION);
    Assertions.assertEquals(HttpMethod.DELETE, cancelRequest.getMethod());
    Assertions.assertEquals("/druid/v2/local-query-id", cancelRequest.getUrl().getPath());
    Assertions.assertTrue(
        cancelRequest.getHeaders().get(QueryResource.HEADER_NATIVE_QUERY_ROUTE)
                     .contains(QueryResource.NATIVE_QUERY_ROUTE_LOCAL)
    );
  }

  /** Closing after the response has been fully received does not cancel the already-finished query. */
  @Test
  public void testCloseAfterCompletedResponseDoesNotCancel() throws Exception
  {
    final List<RequestBuilder> requests = runNodeLocalQueryAndClose(true);

    Assertions.assertEquals(1, requests.size());
    Assertions.assertEquals(HttpMethod.POST, requests.get(0).build(LOCAL_SERVICE_LOCATION).getMethod());
  }

  /** Runs a node-local query against a fake service client, closes it, and returns every request that was sent. */
  private List<RequestBuilder> runNodeLocalQueryAndClose(final boolean completeResponse) throws Exception
  {
    final List<RequestBuilder> requests = new ArrayList<>();
    final ServiceClient localServiceClient = new ServiceClient()
    {
      @Override
      @SuppressWarnings("unchecked")
      public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
          final RequestBuilder requestBuilder,
          final HttpResponseHandler<IntermediateType, FinalType> handler
      )
      {
        requests.add(requestBuilder);
        if (HttpMethod.POST.equals(requestBuilder.build(LOCAL_SERVICE_LOCATION).getMethod())) {
          final ClientResponse<IntermediateType> response = handler.handleResponse(
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
              TestHttpClient.NOOP_TRAFFIC_COP
          );
          if (completeResponse) {
            handler.done(response);
          }
          return Futures.immediateFuture((FinalType) response.getObj());
        }
        return (ListenableFuture<FinalType>) Futures.immediateVoidFuture();
      }

      @Override
      public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
      {
        return this;
      }
    };
    final DataServerClient localTarget = new DataServerClient(
        (serviceName, serviceLocator, retryPolicy) -> localServiceClient,
        LOCAL_SERVICE_LOCATION,
        jsonMapper,
        StandardRetryPolicy.noRetries(),
        true
    );
    final Closer closer = Closer.create();

    localTarget.run(
        (ScanQuery) query.withId("local-query-id"),
        DefaultResponseContext.createEmpty(),
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        closer
    );
    closer.close();
    return requests;
  }

  private static class DataServerResponse
  {
    List<SegmentDescriptor> missingSegments;

    @JsonCreator
    public DataServerResponse(@JsonProperty("missingSegments") List<SegmentDescriptor> missingSegments)
    {
      this.missingSegments = missingSegments;
    }

    @JsonProperty("missingSegments")
    public List<SegmentDescriptor> getMissingSegments()
    {
      return missingSegments;
    }
  }
}
