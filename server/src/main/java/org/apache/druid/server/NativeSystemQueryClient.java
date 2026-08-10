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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.rpc.indexing.NativeSystemQueryResponse;
import org.apache.druid.segment.column.RowSignature;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Fans out provider-row requests for system tables.
 *
 * <p>The Broker owns the query plan, so each component is asked for scan-only rows.  The Broker then executes the
 * original native query over the concatenated {@link org.apache.druid.query.InlineDataSource}.  This keeps grouping,
 * ordering, and window semantics centralized while still pushing provider filters to every component.</p>
 */
public class NativeSystemQueryClient
{
  private static final String SYSTEM_QUERY_PATH = "/druid/v2/system";

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final DruidNode selfNode;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public NativeSystemQueryClient(
      final DruidNodeDiscoveryProvider discoveryProvider,
      @Self final DruidNode selfNode,
      @EscalatedClient final HttpClient httpClient,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.selfNode = selfNode;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Fetches provider rows for a system table from every component that serves it. Components that do not own the
   * requested table return HTTP 501 and are skipped; a disabled endpoint or any other error fails the query.
   */
  public ListenableFuture<NativeSystemQueryResponse> run(final Query<?> query)
  {
    if (!(query.getDataSource() instanceof SystemTableDataSource)) {
      return Futures.immediateFailedFuture(
          new ISE("Only native system-table queries can be sent to the native system query client")
      );
    }

    final Query<?> scanQuery = query.withOverriddenContext(
        ImmutableMap.of(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_SCAN_ONLY, true)
    );
    final List<ListenableFuture<Optional<NativeSystemQueryResponse>>> responses = new ArrayList<>();
    for (final DruidNode node : discoverNodes()) {
      responses.add(runOnNode(node, scanQuery));
    }

    return Futures.transform(
        Futures.allAsList(responses),
        this::mergeResponses,
        MoreExecutors.directExecutor()
    );
  }

  private ListenableFuture<Optional<NativeSystemQueryResponse>> runOnNode(
      final DruidNode node,
      final Query<?> query
  )
  {
    try {
      final URL url = node.getUriToUse().resolve(SYSTEM_QUERY_PATH).toURL();
      final Request request = new Request(HttpMethod.POST, url)
          .addHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
          .addHeader(HttpHeaders.Names.ACCEPT, "application/json")
          .setContent(jsonMapper.writeValueAsBytes(query));
      return Futures.transform(
          httpClient.go(request, new BytesFullResponseHandler()),
          response -> parseResponse(node, response),
          MoreExecutors.directExecutor()
      );
    }
    catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  private Optional<NativeSystemQueryResponse> parseResponse(
      final DruidNode node,
      final BytesFullResponseHolder response
  )
  {
    if (response.getStatus().getCode() == 501) {
      return Optional.empty();
    }
    if (response.getStatus().getCode() != 200) {
      throw new ISE(
          "Native system query on node[%s] failed with HTTP status[%s]: %s",
          node.getHostAndPortToUse(),
          response.getStatus().getCode(),
          new String(response.getContent(), StandardCharsets.UTF_8)
      );
    }
    return Optional.of(
        JacksonUtils.readValue(
            jsonMapper,
            response.getContent(),
            new TypeReference<NativeSystemQueryResponse>() {}
        )
    );
  }

  private NativeSystemQueryResponse mergeResponses(final List<Optional<NativeSystemQueryResponse>> responses)
  {
    final Optional<NativeSystemQueryResponse> firstResponse = responses.stream()
                                                                       .filter(Optional::isPresent)
                                                                       .map(Optional::get)
                                                                       .findFirst();
    if (firstResponse.isEmpty()) {
      throw new ISE("No component serves the requested native system table");
    }

    final RowSignature signature = firstResponse.get().getSignature();
    final List<Object[]> rows = new ArrayList<>();
    for (final Optional<NativeSystemQueryResponse> optionalResponse : responses) {
      if (optionalResponse.isEmpty()) {
        continue;
      }
      final NativeSystemQueryResponse response = optionalResponse.get();
      if (!signature.equals(response.getSignature())) {
        throw new ISE("Native system query response signatures do not match");
      }
      rows.addAll(response.getRows());
    }
    return new NativeSystemQueryResponse(signature, rows);
  }

  private Collection<DruidNode> discoverNodes()
  {
    final Map<String, DruidNode> nodes = new LinkedHashMap<>();
    nodes.put(selfNode.getHostAndPortToUse(), selfNode);
    // A node can advertise multiple roles.  Query it once and let its local provider report all of its roles.
    for (final NodeRole nodeRole : NodeRole.values()) {
      for (final DiscoveryDruidNode discoveryNode : discoveryProvider.getForNodeRole(nodeRole).getAllNodes()) {
        final DruidNode node = discoveryNode.getDruidNode();
        nodes.putIfAbsent(node.getHostAndPortToUse(), node);
      }
    }
    return nodes.values();
  }
}
