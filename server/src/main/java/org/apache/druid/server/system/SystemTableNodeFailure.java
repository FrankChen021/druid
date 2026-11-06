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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.rpc.ServiceNotAvailableException;

import java.io.EOFException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.Set;

/** Classifies failures from node-local system-table queries. */
public class SystemTableNodeFailure
{
  /**
   * Responses meaning the node can't serve the request at all: 404 from a node that doesn't serve the endpoint, and
   * 502 and 503 from a node or proxy that is unavailable. A 400 isn't included: it can be a genuine query error.
   */
  private static final Set<HttpResponseStatus> UNAVAILABLE_STATUSES = Set.of(
      HttpResponseStatus.NOT_FOUND,
      HttpResponseStatus.BAD_GATEWAY,
      HttpResponseStatus.SERVICE_UNAVAILABLE
  );

  private SystemTableNodeFailure()
  {
  }

  /**
   * Returns whether a failure means that the contacted node could not serve the request: it is unreachable or
   * unavailable, or it doesn't serve the query endpoint. Such a node is reported as an error row rather than failing
   * the whole query. Other failures, including bad requests, query cancellation, timeouts, capacity limits, server
   * errors and authorization, must remain query failures.
   */
  public static boolean isAvailabilityFailure(final Throwable failure)
  {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof SocketException
          || cause instanceof UnknownHostException
          || cause instanceof EOFException
          || cause instanceof ClosedChannelException
          || cause instanceof ChannelException
          || cause instanceof ServiceNotAvailableException
          || cause instanceof ServiceClosedException
          || isUnavailableHttpFailure(cause)) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private static boolean isUnavailableHttpFailure(final Throwable failure)
  {
    return failure instanceof HttpResponseException httpFailure
           && UNAVAILABLE_STATUSES.contains(httpFailure.getResponse().getStatus());
  }
}
