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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Pattern;

/// Per-ingestion endpoint, outbound authentication, and bounded HTTP settings.
public class RemoteDruidConnection
{
  private static final Pattern TRAILING_SLASHES = Pattern.compile("/+$");
  private final URI endpoint;
  private final RemoteDruidAuthentication authentication;
  private final int connectTimeout;
  private final int readTimeout;
  private final int maxRetries;

  @JsonCreator
  public RemoteDruidConnection(
      @JsonProperty("endpoint") final URI endpoint,
      @JsonProperty("authentication") @Nullable final RemoteDruidAuthentication authentication,
      @JsonProperty("connectTimeout") @Nullable final Integer connectTimeout,
      @JsonProperty("readTimeout") @Nullable final Integer readTimeout,
      @JsonProperty("maxRetries") @Nullable final Integer maxRetries
  )
  {
    Preconditions.checkArgument(endpoint != null && endpoint.getHost() != null, "An HTTP(S) endpoint is required");
    Preconditions.checkArgument(endpoint.getUserInfo() == null && endpoint.getQuery() == null && endpoint.getFragment() == null,
                                "Provide authentication separately from the endpoint");
    final String value = TRAILING_SLASHES.matcher(endpoint.toString()).replaceAll("");
    this.endpoint = URI.create(value.endsWith("/druid/v2") ? value : value + "/druid/v2");
    this.authentication = authentication == null ? new RemoteDruidAuthentication.None() : authentication;
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
  public RemoteDruidAuthentication getAuthentication()
  {
    return authentication;
  }

  @JsonProperty
  public int getConnectTimeout()
  {
    return connectTimeout;
  }

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

  @Override
  public boolean equals(final Object other)
  {
    return other instanceof RemoteDruidConnection that && endpoint.equals(that.endpoint)
           && authentication.equals(that.authentication) && connectTimeout == that.connectTimeout
           && readTimeout == that.readTimeout && maxRetries == that.maxRetries;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(endpoint, authentication, connectTimeout, readTimeout, maxRetries);
  }
}
