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

package org.apache.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;

class HttpResponseWriter
{
  private final String responseType;
  private final ObjectMapper inputMapper;
  private final ObjectMapper serializeDateTimeAsLongInputMapper;
  private final boolean isPretty;

  HttpResponseWriter(
      String responseType,
      ObjectMapper inputMapper,
      ObjectMapper serializeDateTimeAsLongInputMapper,
      boolean isPretty
  )
  {
    this.responseType = responseType;
    this.inputMapper = inputMapper;
    this.serializeDateTimeAsLongInputMapper = serializeDateTimeAsLongInputMapper;
    this.isPretty = isPretty;
  }

  String getResponseType()
  {
    return responseType;
  }

  ObjectWriter newOutputWriter(
      @Nullable QueryToolChest toolChest,
      @Nullable Query query,
      boolean serializeDateTimeAsLong
  )
  {
    final ObjectMapper mapper = serializeDateTimeAsLong ? serializeDateTimeAsLongInputMapper : inputMapper;
    final ObjectMapper decoratedMapper;
    if (toolChest != null) {
      decoratedMapper = toolChest.decorateObjectMapper(mapper, Preconditions.checkNotNull(query, "query"));
    } else {
      decoratedMapper = mapper;
    }
    return isPretty ? decoratedMapper.writerWithDefaultPrettyPrinter() : decoratedMapper.writer();
  }

  Response ok(Object object) throws IOException
  {
    return Response.ok(newOutputWriter(null, null, false).writeValueAsString(object), responseType).build();
  }

  Response gotError(Exception e) throws IOException
  {
    return buildNonOkResponse(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        QueryInterruptedException.wrapIfNeeded(e)
    );
  }

  Response gotTimeout(QueryTimeoutException e) throws IOException
  {
    return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, e);
  }

  Response gotLimited(QueryCapacityExceededException e) throws IOException
  {
    return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, e);
  }

  Response gotUnsupported(QueryUnsupportedException e) throws IOException
  {
    return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, e);
  }

  Response gotBadQuery(BadQueryException e) throws IOException
  {
    return buildNonOkResponse(BadQueryException.STATUS_CODE, e);
  }

  Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
  {
    return Response.status(status)
                   .type(responseType)
                   .entity(newOutputWriter(null, null, false).writeValueAsBytes(e))
                   .build();
  }
}
