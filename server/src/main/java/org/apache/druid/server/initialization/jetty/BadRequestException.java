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

package org.apache.druid.server.initialization.jetty;

import javax.ws.rs.core.Response;

/**
 * Throw this when a request is illegal, and we want to send a 400 response back,
 * Jersey exception mapper {@link ResponseStatusExceptionMapper} will take care of sending the response.
 *
 */
public class BadRequestException extends ResponseStatusException
{
  public BadRequestException(String msg)
  {
    super(Response.Status.BAD_REQUEST, msg);
  }
}
