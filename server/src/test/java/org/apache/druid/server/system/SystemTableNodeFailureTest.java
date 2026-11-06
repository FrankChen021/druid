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


package org.apache.druid.server.system;

import io.netty.channel.ChannelException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.rpc.ServiceNotAvailableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;

public class SystemTableNodeFailureTest
{
  /** Transport-level failures mean that the contacted node is unavailable. */
  @Test
  public void testTransportFailuresAreAvailabilityFailures()
  {
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new SocketException("connection refused")));
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new UnknownHostException("host")));
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new EOFException()));
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new ClosedChannelException()));
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new ChannelException("closed")));
    Assertions.assertTrue(
        SystemTableNodeFailure.isAvailabilityFailure(new RuntimeException("wrapped", new SocketException("reset")))
    );
  }

  /** A node that is unreachable through the RPC layer is unavailable, whatever the reason. */
  @Test
  public void testServiceUnavailableFailuresAreAvailabilityFailures()
  {
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new ServiceNotAvailableException("node")));
    Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(new ServiceClosedException("node")));
  }

  /**
   * A node that doesn't serve the endpoint (404), or that a proxy reports as unavailable (502, 503), is reported as a
   * per-node error row like any other unhealthy node.
   */
  @Test
  public void testMissingEndpointAndUnavailableResponsesAreAvailabilityFailures()
  {
    for (final HttpResponseStatus status : new HttpResponseStatus[]{
        HttpResponseStatus.NOT_FOUND,
        HttpResponseStatus.BAD_GATEWAY,
        HttpResponseStatus.SERVICE_UNAVAILABLE
    }) {
      Assertions.assertTrue(SystemTableNodeFailure.isAvailabilityFailure(httpFailure(status)), status.toString());
    }
  }

  /** Authorization failures, timeouts and unexpected errors stay query failures. */
  @Test
  public void testAuthorizationTimeoutAndUnexpectedFailuresAreQueryFailures()
  {
    Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(httpFailure(HttpResponseStatus.UNAUTHORIZED)));
    Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(httpFailure(HttpResponseStatus.FORBIDDEN)));
    // A bad request, a query timeout, a capacity limit and a server error on the node are query failures.
    for (final HttpResponseStatus status : new HttpResponseStatus[]{
        HttpResponseStatus.BAD_REQUEST,
        HttpResponseStatus.GATEWAY_TIMEOUT,
        HttpResponseStatus.TOO_MANY_REQUESTS,
        HttpResponseStatus.INTERNAL_SERVER_ERROR
    }) {
      Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(httpFailure(status)), status.toString());
    }
    Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(new RpcException("forbidden")));
    Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(new QueryTimeoutException()));
    Assertions.assertFalse(SystemTableNodeFailure.isAvailabilityFailure(new IllegalStateException("bug")));
  }

  private static HttpResponseException httpFailure(final HttpResponseStatus status)
  {
    return new HttpResponseException(
        new StringFullResponseHolder(new DefaultHttpResponse(HttpVersion.HTTP_1_1, status), StandardCharsets.UTF_8)
    );
  }
}
