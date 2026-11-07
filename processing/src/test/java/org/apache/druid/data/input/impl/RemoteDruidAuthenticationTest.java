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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RemoteDruidAuthenticationTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testBasicAuthenticationAcceptsOnlyPlainPasswords() throws Exception
  {
    // A server-side provider would let a query send the server's secrets to an endpoint of its choice.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new RemoteDruidAuthentication.Basic(
            "reader",
            new EnvironmentVariablePasswordProvider("AWS_SECRET_ACCESS_KEY")
        )
    );
    final JsonMappingException e = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(
            """
            {"type":"basic","username":"reader","password":{"type":"environment","variable":"X"}}
            """,
            RemoteDruidAuthentication.class
        )
    );
    Assertions.assertTrue(e.getMessage().contains("accepts only a plain password"), e.getMessage());

    final RemoteDruidAuthentication plain = mapper.readValue(
        """
        {"type":"basic","username":"reader","password":"secret"}
        """,
        RemoteDruidAuthentication.class
    );
    Assertions.assertEquals("secret", ((RemoteDruidAuthentication.Basic) plain).getPassword().getPassword());
  }
}
