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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

public class HttpReaderWriterBuilder
{
  private final ObjectMapper serializeDateTimeAsLongJsonMapper;
  private final ObjectMapper serializeDateTimeAsLongSmileMapper;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;

  @Inject
  public HttpReaderWriterBuilder(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.serializeDateTimeAsLongJsonMapper = serializeDataTimeAsLong(jsonMapper);
    this.serializeDateTimeAsLongSmileMapper = serializeDataTimeAsLong(smileMapper);
  }

  public HttpReaderWriter create(
      HttpServletRequest httpRequest,
      String pretty
  )
  {
    String requestType = httpRequest.getContentType();
    String acceptHeader = httpRequest.getHeader("Accept");

    // response type defaults to Content-Type if 'Accept' header not provided
    String responseType = Strings.isNullOrEmpty(acceptHeader) ? requestType : acceptHeader;

    boolean isRequestSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType)
                             || QueryResource.APPLICATION_SMILE.equals(requestType);
    boolean isResponseSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(responseType)
                              || QueryResource.APPLICATION_SMILE.equals(responseType);

    return new HttpReaderWriter(
        new HttpRequestReader(isRequestSmile ? smileMapper : jsonMapper),
        new HttpResponseWriter(
            isResponseSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON,
            isResponseSmile ? smileMapper : jsonMapper,
            isResponseSmile ? serializeDataTimeAsLong(smileMapper) : serializeDataTimeAsLong(jsonMapper),
            pretty != null
        )
    );
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }
}
