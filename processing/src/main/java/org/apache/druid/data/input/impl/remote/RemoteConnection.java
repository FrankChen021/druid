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

package org.apache.druid.data.input.impl.remote;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Per-ingestion endpoint, outbound authentication, and bounded HTTP settings. The TLS context is injected, not
 * serialized.
 */
public class RemoteConnection
{
  private static final Logger LOG = new Logger(RemoteConnection.class);
  private static final Pattern TRAILING_SLASHES = Pattern.compile("/+$");

  /**
   * Bounds a best-effort cancel, so that a hung source does not hold up the failure that abandoned its query.
   */
  static final int CANCEL_TIMEOUT_MILLIS = 10_000;
  private final URI endpoint;
  private final RemoteAuthentication authentication;
  private final int connectTimeout;
  private final int readTimeout;
  private final int maxRetries;
  private final RemoteTlsContext tlsContext;

  @JsonCreator
  public RemoteConnection(
      @JsonProperty("endpoint") final URI endpoint,
      @JsonProperty("authentication") @Nullable final RemoteAuthentication authentication,
      @JsonProperty("connectTimeout") @Nullable final Integer connectTimeout,
      @JsonProperty("readTimeout") @Nullable final Integer readTimeout,
      @JsonProperty("maxRetries") @Nullable final Integer maxRetries,
      @JacksonInject @Nullable final RemoteTlsContext tlsContext
  )
  {
    Preconditions.checkArgument(
        endpoint != null && endpoint.getScheme() != null && endpoint.getHost() != null,
        "An HTTP(S) endpoint is required"
    );
    this.tlsContext = tlsContext == null ? new RemoteTlsContext() : tlsContext;
    Preconditions.checkArgument(
        endpoint.getUserInfo() == null && endpoint.getQuery() == null && endpoint.getFragment() == null,
        "Provide authentication separately from the endpoint"
    );
    String value = TRAILING_SLASHES.matcher(endpoint.toString()).replaceAll("");

    // Accept the SQL endpoint too, since that is the API this connection is mostly used for.
    if (value.endsWith("/druid/v2/sql")) {
      value = value.substring(0, value.length() - "/sql".length());
    }
    this.endpoint = URI.create(value.endsWith("/druid/v2") ? value : value + "/druid/v2");
    this.authentication = authentication == null ? new RemoteAuthentication.None() : authentication;
    this.connectTimeout = connectTimeout == null ? 10_000 : connectTimeout;

    // Streamed SQL results may not produce a byte until the source finishes an aggregation.
    this.readTimeout = readTimeout == null ? 300_000 : readTimeout;
    this.maxRetries = maxRetries == null ? 2 : maxRetries;
    Preconditions.checkArgument(this.connectTimeout > 0 && this.readTimeout > 0, "Timeouts must be positive");
    Preconditions.checkArgument(this.maxRetries >= 0 && this.maxRetries <= 10, "maxRetries must be between 0 and 10");
  }

  @JsonProperty
  public URI getEndpoint()
  {
    return endpoint;
  }

  @JsonProperty
  public RemoteAuthentication getAuthentication()
  {
    return authentication;
  }

  /**
   * HTTP connection timeout, in milliseconds.
   */
  @JsonProperty
  public int getConnectTimeout()
  {
    return connectTimeout;
  }

  /**
   * Maximum time to wait for response data, in milliseconds.
   */
  @JsonProperty
  public int getReadTimeout()
  {
    return readTimeout;
  }

  @JsonProperty
  public int getMaxRetries()
  {
    return maxRetries;
  }

  /**
   * Creates an authenticated transport for this connection with the given read timeout in milliseconds. Connection
   * setup, including the TLS handshake, is bounded by the connect timeout or the read timeout, whichever is shorter.
   */
  public RemoteHttpClient createClient(final int readTimeout)
  {
    return authentication.createClient(Math.min(connectTimeout, readTimeout), readTimeout, tlsContext.get());
  }

  /**
   * Asks the source to cancel a query, on a separate client with a short timeout. Best effort: a failure is ignored.
   *
   * @param queryEndpoint the endpoint the query was posted to, such as the SQL endpoint for a SQL query
   */
  void cancel(final URI queryEndpoint, final String queryId)
  {
    // Abandoned queries are often interrupted. Clear the flag while cancelling, so the request is not abandoned too.
    final boolean interrupted = Thread.interrupted();
    try (final RemoteHttpClient client = createClient(Math.min(readTimeout, CANCEL_TIMEOUT_MILLIS));
         final RemoteHttpClient.Response ignored = client.execute(
             URI.create(queryEndpoint + "/" + StringUtils.urlEncode(queryId)), "DELETE", null
         )) {
      // The source query also has a bounded timeout.
    }
    catch (Exception e) {
      // Preserve the original failure and avoid logging provider exceptions that may contain credentials.
      LOG.debug("Unable to cancel remote Druid query[%s]", queryId);
    }
    finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public boolean equals(final Object other)
  {
    return other instanceof RemoteConnection that && endpoint.equals(that.endpoint)
           && authentication.equals(that.authentication) && connectTimeout == that.connectTimeout
           && readTimeout == that.readTimeout && maxRetries == that.maxRetries;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(endpoint, authentication, connectTimeout, readTimeout, maxRetries);
  }
}
