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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.PasswordProvider;

/** Outbound authentication provider. Extensions can register additional Jackson subtypes. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "none", value = RemoteDruidAuthentication.None.class),
    @JsonSubTypes.Type(name = "basic", value = RemoteDruidAuthentication.Basic.class)
})
public interface RemoteDruidAuthentication
{
  RemoteDruidHttpClient createClient(int connectTimeout, int readTimeout);

  class None implements RemoteDruidAuthentication
  {
    @Override
    public RemoteDruidHttpClient createClient(final int connectTimeout, final int readTimeout)
    {
      return new RemoteDruidHttpClient.UrlConnectionClient(null, null, connectTimeout, readTimeout);
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

  class Basic implements RemoteDruidAuthentication
  {
    private final String username;
    private final PasswordProvider password;

    @JsonCreator
    public Basic(
        @JsonProperty("username") final String username,
        @JsonProperty("password") final PasswordProvider password
    )
    {
      this.username = Preconditions.checkNotNull(username, "username");
      this.password = Preconditions.checkNotNull(password, "password");
    }

    @JsonProperty
    public String getUsername()
    {
      return username;
    }

    @JsonProperty
    public PasswordProvider getPassword()
    {
      return password;
    }

    @Override
    public RemoteDruidHttpClient createClient(final int connectTimeout, final int readTimeout)
    {
      return new RemoteDruidHttpClient.UrlConnectionClient(username, password, connectTimeout, readTimeout);
    }

    @Override
    public boolean equals(final Object other)
    {
      return other instanceof Basic that && username.equals(that.username) && password.equals(that.password);
    }

    @Override
    public int hashCode()
    {
      return java.util.Objects.hash(username, password);
    }
  }
}
