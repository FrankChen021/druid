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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RemoteAuthenticationTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  /**
   * Guards against a query using the remote input source to exfiltrate secrets from the server that runs it.
   * The query chooses the remote URL, and a password provider other than the default resolves its value on the server.
   * An environment provider would therefore make the worker send its own environment variable, such as a cloud
   * credential, as the Basic password to a host the query's author controls.
   * <p>
   * Basic authentication must accept only a plain password given in the request.
   * The environment provider stands in for every non-default provider, since one type check rejects them all.
   */
  @Test
  public void testBasicAuthenticationRejectsPasswordProviders()
  {
    final JsonMappingException e = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(
            """
            {"type":"basic","userName":"reader","password":{"type":"environment","variable":"X"}}
            """,
            RemoteAuthentication.class
        )
    );
    Assertions.assertTrue(e.getMessage().contains("accepts only a plain password"), e.getMessage());
  }
}
