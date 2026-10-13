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
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.router.QueryHostFinder;
import org.apache.druid.server.router.RendezvousHashAvaticaConnectionBalancer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/** Tests the Router's node-local system-table dispatch without starting a Jetty server. */
public class AsyncQueryForwardingServletNodeLocalTest
{
  /**
   * Once the request has been handed to the local container, a failure while it streams the response (for example a
   * client disconnect) is not a query-parse failure: it must reach the servlet container instead of being logged as
   * an unparseable query and answered with an error body on a response that is already being written.
   */
  @Test
  public void testLocalContainerFailureIsNotReportedAsQueryParseFailure() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("server_properties"))
                                  .eternityInterval()
                                  .build();
    final byte[] body = jsonMapper.writeValueAsBytes(query);

    final RequestLogger requestLogger = Mockito.mock(RequestLogger.class);
    final GuiceContainer localQueryContainer = Mockito.mock(GuiceContainer.class);
    Mockito.doThrow(new IOException("Broken pipe"))
           .when(localQueryContainer)
           .service(Mockito.any(HttpServletRequest.class), Mockito.any(HttpServletResponse.class));

    final AsyncQueryForwardingServlet servlet = new AsyncQueryForwardingServlet(
        new MapQueryToolChestWarehouse(ImmutableMap.of()),
        jsonMapper,
        new DefaultObjectMapper(),
        new QueryHostFinder(null, new RendezvousHashAvaticaConnectionBalancer()),
        null,
        new DruidHttpClientConfig(),
        Mockito.mock(ServiceEmitter.class),
        requestLogger,
        new DefaultGenericQueryMetricsFactory(),
        new AuthenticatorMapper(ImmutableMap.of()),
        new Properties(),
        new ServerConfig()
    );
    servlet.setLocalQueryContainer(localQueryContainer);

    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRequestURI()).thenReturn("/druid/v2/");
    Mockito.when(request.getMethod()).thenReturn("POST");
    Mockito.when(request.getHeader(QueryResource.HEADER_NATIVE_QUERY_ROUTE))
           .thenReturn(QueryResource.NATIVE_QUERY_ROUTE_LOCAL);
    Mockito.when(request.getInputStream()).thenReturn(servletInputStream(body));
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult("user", "allowAll", null, null));
    final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getOutputStream()).thenReturn(new ServletOutputStream()
    {
      @Override
      public boolean isReady()
      {
        return true;
      }

      @Override
      public void setWriteListener(final WriteListener writeListener)
      {
      }

      @Override
      public void write(final int b)
      {
      }
    });

    Assertions.assertThrows(IOException.class, () -> servlet.service(request, response));

    Mockito.verifyNoInteractions(requestLogger);
    Mockito.verify(response, Mockito.never()).setStatus(Mockito.anyInt());
  }

  private static ServletInputStream servletInputStream(final byte[] bytes)
  {
    final ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    return new ServletInputStream()
    {
      @Override
      public boolean isFinished()
      {
        return input.available() == 0;
      }

      @Override
      public boolean isReady()
      {
        return true;
      }

      @Override
      public void setReadListener(final ReadListener readListener)
      {
      }

      @Override
      public int read()
      {
        return input.read();
      }
    };
  }
}
