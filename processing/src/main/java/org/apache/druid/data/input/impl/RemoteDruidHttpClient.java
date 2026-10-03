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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

/** Transport boundary allows authentication providers to perform more than header decoration. */
public interface RemoteDruidHttpClient extends AutoCloseable
{
  Response execute(URI endpoint, String method, @Nullable byte[] body) throws IOException;

  @Override
  default void close()
  {
    // The URL connection transport has no resources beyond each response.
  }

  interface Response extends AutoCloseable
  {
    int status();

    InputStream body() throws IOException;

    @Override
    void close();
  }

  class UrlConnectionClient implements RemoteDruidHttpClient
  {
    @Nullable
    private final String username;
    @Nullable
    private final PasswordProvider password;
    private final int connectTimeout;
    private final int readTimeout;

    public UrlConnectionClient(
        @Nullable final String username,
        @Nullable final PasswordProvider password,
        final int connectTimeout,
        final int readTimeout
    )
    {
      this.username = username;
      this.password = password;
      this.connectTimeout = connectTimeout;
      this.readTimeout = readTimeout;
    }

    @Override
    public Response execute(final URI endpoint, final String method, @Nullable final byte[] body) throws IOException
    {
      final HttpURLConnection http = (HttpURLConnection) endpoint.toURL().openConnection();
      boolean success = false;
      try {
        http.setInstanceFollowRedirects(false);
        http.setConnectTimeout(connectTimeout);
        http.setReadTimeout(readTimeout);
        http.setRequestMethod(method);
        if (username != null && password != null) {
          final String credentials = username + ':' + password.getPassword();
          http.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(StringUtils.toUtf8(credentials)));
        }
        if (body != null) {
          http.setDoOutput(true);
          http.setRequestProperty("Content-Type", "application/json");
          http.setFixedLengthStreamingMode(body.length);
          try (final OutputStream out = http.getOutputStream()) {
            out.write(body);
          }
        }
        final int status = http.getResponseCode();
        success = true;
        return new Response()
        {
          @Override
          public int status()
          {
            return status;
          }

          @Override
          public InputStream body() throws IOException
          {
            return http.getInputStream();
          }

          @Override
          public void close()
          {
            http.disconnect();
          }
        };
      }
      finally {
        if (!success) {
          http.disconnect();
        }
      }
    }
  }
}
