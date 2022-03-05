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

import org.apache.druid.java.util.common.StringUtils;

import javax.ws.rs.core.Response;

/**
 * This class is for any exceptions that should return a not found status code (404).
 *
 * @see ResponseStatusExceptionMapper
 */
public class NotFoundException extends ResponseStatusException
{
  public NotFoundException(String msg)
  {
    super(Response.Status.NOT_FOUND, msg);
  }

  public static Response toResponse(String message)
  {
    return toResponse(Response.Status.NOT_FOUND, message);
  }

  public static Response toResponse(String messageFormat, Object... formatArgs)
  {
    return toResponse(Response.Status.NOT_FOUND, StringUtils.format(messageFormat, formatArgs));
  }
}