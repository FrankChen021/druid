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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ServerConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.util.HashMap;
import java.util.Map;

public class StandardResponseHeaderFilterHolderTest
{
  private static final DruidNode SELF_NODE = new DruidNode(
      "druid/test",
      "test-host",
      false,
      8080,
      null,
      true,
      false
  );

  public ServerConfig serverConfig;
  public HttpServletRequest httpRequest;
  public HttpServletResponse httpResponse;
  public FilterChain filterChain;

  public HttpServletResponse proxyResponse;
  public Response clientResponse;

  @BeforeEach
  public void setUp()
  {
    serverConfig = EasyMock.strictMock(ServerConfig.class);
    httpRequest = EasyMock.strictMock(HttpServletRequest.class);
    httpResponse = EasyMock.strictMock(HttpServletResponse.class);
    filterChain = EasyMock.strictMock(FilterChain.class);

    proxyResponse = EasyMock.strictMock(HttpServletResponse.class);
    clientResponse = EasyMock.strictMock(Response.class);
  }

  @AfterEach
  public void tearDown()
  {
    EasyMock.verify(serverConfig, httpRequest, httpResponse, filterChain, proxyResponse, clientResponse);
  }

  @Test
  public void test_get_nullContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'none'")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, "test-host:8080")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, "druid/test")
                    .build()
    );
  }

  @Test
  public void test_post_nullContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.POST).anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.of(
            StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER,
            "test-host:8080",
            StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER,
            "druid/test"
        )
    );
  }

  @Test
  public void test_get_emptyContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'none'")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, "test-host:8080")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, "druid/test")
                    .build()
    );
  }

  @Test
  public void test_get_overrideContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("frame-ancestors 'self'").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.expect(httpResponse.getContentType()).andReturn("text/html").anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'self'")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, "test-host:8080")
                    .put(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, "druid/test")
                    .build()
    );
  }

  @Test
  public void test_get_invalidContentSecurityPolicy()
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("erroné").once();

    replayAllMocks();

    final RuntimeException e = Assertions.assertThrows(RuntimeException.class, this::makeFilter);

    Assertions.assertNotNull(e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("Content-Security-Policy header value must be fully ASCII"));
  }

  @Test
  public void test_deduplicateHeadersInProxyServlet_withDuplicates()
  {
    EasyMock.expect(proxyResponse.containsHeader("Cache-Control")).andReturn(true).once();
    proxyResponse.setHeader("Cache-Control", null);
    EasyMock.expectLastCall().once();
    EasyMock.expect(proxyResponse.containsHeader("Strict-Transport-Security")).andReturn(false).once();
    EasyMock.expect(proxyResponse.containsHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER))
            .andReturn(true).once();
    proxyResponse.setHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, null);
    EasyMock.expectLastCall().once();
    EasyMock.expect(proxyResponse.containsHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER))
            .andReturn(true).once();
    proxyResponse.setHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, null);
    EasyMock.expectLastCall().once();

    EasyMock.expect(clientResponse.getHeaders())
            .andReturn(
                HttpFields.from(
                    new HttpField("Cache-Control", "true"),
                    new HttpField("Strict-Transport-Security", "true"),
                    new HttpField(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, "upstream:8082"),
                    new HttpField(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, "druid/broker")
                )
            ).times(5);

    replayAllMocks();

    StandardResponseHeaderFilterHolder.deduplicateHeadersInProxyServlet(proxyResponse, clientResponse);
  }

  @Test
  public void test_duplicateHeadersInProxyServlet_withNoDuplicates()
  {
    EasyMock.expect(proxyResponse.containsHeader("Cache-Control")).andReturn(false).once();
    EasyMock.expect(proxyResponse.containsHeader("Strict-Transport-Security")).andReturn(false).once();
    EasyMock.expect(proxyResponse.containsHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER))
            .andReturn(false).once();
    EasyMock.expect(proxyResponse.containsHeader(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER))
            .andReturn(false).once();

    EasyMock.expect(clientResponse.getHeaders())
            .andReturn(HttpFields.from(
                new HttpField("Cache-Control", "true"),
                new HttpField("Strict-Transport-Security", "true"),
                new HttpField(StandardResponseHeaderFilterHolder.RESPONSE_SERVER_HEADER, "upstream:8082"),
                new HttpField(StandardResponseHeaderFilterHolder.RESPONSE_SERVICE_HEADER, "druid/broker")
            ))
            .times(5);

    replayAllMocks();

    StandardResponseHeaderFilterHolder.deduplicateHeadersInProxyServlet(proxyResponse, clientResponse);
  }

  private StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter makeFilter()
  {
    return (StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter)
        new StandardResponseHeaderFilterHolder(serverConfig, SELF_NODE).getFilter();
  }

  private void runFilterAndVerifyHeaders(final Map<String, String> expectedHeaders) throws Exception
  {
    final Map<String, Capture<String>> captureMap = new HashMap<>();

    for (final Map.Entry<String, String> entry : expectedHeaders.entrySet()) {
      final String headerName = entry.getKey();
      final Capture<String> headerValueCapture = Capture.newInstance();
      captureMap.put(headerName, headerValueCapture);

      httpResponse.setHeader(EasyMock.eq(headerName), EasyMock.capture(headerValueCapture));
      EasyMock.expectLastCall();
    }

    filterChain.doFilter(httpRequest, httpResponse);
    EasyMock.expectLastCall();

    replayAllMocks();
    final StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter filter = makeFilter();
    filter.doFilter(httpRequest, httpResponse, filterChain);

    for (final Map.Entry<String, String> entry : expectedHeaders.entrySet()) {
      Assertions.assertEquals(
          entry.getValue(),
          captureMap.get(entry.getKey()).getValue(),
          entry.getKey()
      );
    }
  }

  private void replayAllMocks()
  {
    EasyMock.replay(serverConfig, httpRequest, httpResponse, filterChain, proxyResponse, clientResponse);
  }
}
