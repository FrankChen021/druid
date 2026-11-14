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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

/**
 * A remote Druid request that the source answered with an HTTP status other than 200.
 */
public class RemoteStatusException extends IOException
{
  private static final int MAX_ERROR_BYTES = 64 * 1024;
  private static final int MAX_MESSAGE_CHARS = 1024;

  private final int status;

  public RemoteStatusException(final URI endpoint, final int status, @Nullable final String detail)
  {
    super(message(endpoint, status, detail));
    this.status = status;
  }

  public int getStatus()
  {
    return status;
  }

  /**
   * Whether the status reports a transient condition, such as a timeout, throttling, or a server error. A 501, which
   * Druid returns for unsupported queries, is not transient.
   */
  public boolean isRetryable()
  {
    return status == 408 || status == 429 || (status >= 500 && status != 501);
  }

  /**
   * Whether the status reports a problem with the request itself, such as its credentials, permissions, or SQL.
   */
  public boolean isUserError()
  {
    return (status >= 400 && status < 500 || status == 501) && !isRetryable();
  }

  /**
   * Reads the message of a Druid error response, or returns null if the body has none.
   */
  @Nullable
  static String readMessage(final InputStream in, final ObjectMapper mapper)
  {
    try {
      final byte[] bytes = in.readNBytes(MAX_ERROR_BYTES);
      final Map<String, Object> error = mapper.readValue(bytes, new TypeReference<>() {});
      final Object message = error.getOrDefault("errorMessage", error.get("error"));
      return message == null ? null : StringUtils.chop(String.valueOf(message), MAX_MESSAGE_CHARS);
    }
    catch (Exception e) {
      return null;
    }
  }

  private static String message(final URI endpoint, final int status, @Nullable final String detail)
  {
    final String summary;
    if (status == 401) {
      summary = StringUtils.format(
          "Remote Druid at [%s] rejected the credentials with HTTP status[401]; check the authentication type, user name, and password",
          endpoint
      );
    } else if (status == 403) {
      summary = StringUtils.format(
          "Remote Druid at [%s] denied access with HTTP status[403]; the remote user needs read access to the"
          + " datasource and permission to run SQL queries",
          endpoint
      );
    } else {
      summary = StringUtils.format("Remote Druid at [%s] failed the request with HTTP status[%d]", endpoint, status);
    }
    return detail == null ? summary : summary + ": " + detail;
  }
}
