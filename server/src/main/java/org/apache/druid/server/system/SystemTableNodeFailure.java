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

import java.io.EOFException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;

/** Classifies failures from node-local system-table queries. */
public class SystemTableNodeFailure
{
  private SystemTableNodeFailure()
  {
  }

  /**
   * Returns whether a failure means that the contacted node became unavailable. Other failures, including query
   * cancellation, timeout, authorization, and resource limits, must remain query failures.
   */
  public static boolean isAvailabilityFailure(final Throwable failure)
  {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof SocketException
          || cause instanceof UnknownHostException
          || cause instanceof EOFException
          || cause instanceof ClosedChannelException
          || cause instanceof ChannelException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }
}
