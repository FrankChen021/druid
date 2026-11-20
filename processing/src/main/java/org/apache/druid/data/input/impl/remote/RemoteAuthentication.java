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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;

import javax.net.ssl.SSLContext;
import java.util.Objects;

/**
 * Outbound authentication provider. Extensions can register additional Jackson subtypes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "none", value = RemoteAuthentication.None.class),
    @JsonSubTypes.Type(name = "basic", value = RemoteAuthentication.Basic.class)
})
public interface RemoteAuthentication
{
  /**
   * Creates an authenticated transport with connection and read timeouts in milliseconds, using the given TLS context.
   * The caller closes the client after the operation and closes each response before closing the client.
   */
  RemoteHttpClient createClient(int connectTimeout, int readTimeout, SSLContext sslContext);

  class None implements RemoteAuthentication
  {
    @Override
    public RemoteHttpClient createClient(final int connectTimeout, final int readTimeout, final SSLContext sslContext)
    {
      return new RemoteHttpClient.DruidHttpClient(null, null, connectTimeout, readTimeout, sslContext);
    }

    @Override
    public boolean equals(final Object other)
    {
      return other instanceof None;
    }

    @Override
    public int hashCode()
    {
      return None.class.hashCode();
    }
  }

  class Basic implements RemoteAuthentication
  {
    private final String userName;
    private final PasswordProvider password;

    @JsonCreator
    public Basic(
        @JsonProperty("userName") final String userName,
        @JsonProperty("password") final PasswordProvider password
    )
    {
      this.userName = Preconditions.checkNotNull(userName, "userName");
      this.password = Preconditions.checkNotNull(password, "password");

      // Accept only a plain password given in the request, to keep a query from exfiltrating the server's secrets.
      // The query chooses the remote URL, and providers other than the default resolve their value on the server.
      // For example, {"type": "environment", "variable": "AWS_SECRET_ACCESS_KEY"} would make the worker read its own
      // environment variable and send it in the Authorization header to a host the query's author controls.
      Preconditions.checkArgument(
          password instanceof DefaultPasswordProvider,
          "Remote Druid Basic authentication accepts only a plain password"
      );
    }

    @JsonProperty
    public String getUserName()
    {
      return userName;
    }

    @JsonProperty
    public PasswordProvider getPassword()
    {
      return password;
    }

    @Override
    public RemoteHttpClient createClient(final int connectTimeout, final int readTimeout, final SSLContext sslContext)
    {
      return new RemoteHttpClient.DruidHttpClient(userName, password, connectTimeout, readTimeout, sslContext);
    }

    @Override
    public boolean equals(final Object other)
    {
      return other instanceof Basic that && userName.equals(that.userName) && password.equals(that.password);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(userName, password);
    }
  }
}
