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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Exercises the SQL planner, HTTP native queries, MSQ workers, and target segment publication together. */
public class MSQRemoteDruidInputSourceTest extends MSQTestBase
{
  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private String expectedAuthorization;

  @BeforeEach
  public void startSource() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        Assertions.assertEquals(expectedAuthorization, exchange.getRequestHeaders().getFirst("Authorization"));
        if ("DELETE".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
          return;
        }
        final JsonNode request = objectMapper.readTree(exchange.getRequestBody());
        requests.add(request);
        final Query<?> parsed = objectMapper.treeToValue(request, Query.class);
        // Expose the fixture's three primitive columns in native schema discovery.
        final Query<?> query = parsed instanceof SegmentMetadataQuery metadata
                               ? metadata.withColumns(new ListColumnIncluderator(List.of("__time", "dim1", "cnt"))) : parsed;
        final List<?> result = QueryPlus.wrap(query).run(queryFramework().walker(), ResponseContext.createEmpty()).toList();
        final byte[] response = objectMapper.writeValueAsBytes(result);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
      }
    });
    server.start();
  }

  @AfterEach
  public void stopSource()
  {
    server.stop(0);
  }

  private String external()
  {
    return "TABLE(REMOTE(endpoint => 'http://localhost:" + server.getAddress().getPort()
           + "', dataSource => 'foo', splitDurationMillis => 31536000000))"
           + " EXTEND (__time BIGINT, dim1 VARCHAR, cnt BIGINT)";
  }

  @Test
  public void testInsertSelectStarWithFilterAndDiscoveredIntervals()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external() + " WHERE dim1 = 'abc' PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}))
        .verifyResults();
    Assertions.assertFalse(requests.stream().anyMatch(r -> "segmentMetadata".equals(r.path("queryType").asText())));
    Assertions.assertEquals(1, requests.stream().filter(r -> "timeBoundary".equals(r.path("queryType").asText())).count());
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(2, scans.size());
    Assertions.assertTrue(scans.stream().allMatch(r -> r.path("filter").isMissingNode()));
  }

  @Test
  public void testTimePredicateNarrowsRemoteRead()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + external()
                + " WHERE __time >= TIMESTAMP '2001-01-03 00:00:00' PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("dim1", ColumnType.STRING).add("cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), "abc", 1L}))
        .verifyResults();
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(1, scans.size());
    Assertions.assertTrue(scans.get(0).path("intervals").get(0).asText().startsWith("2001-01-03T00:00:00.000Z/"));
  }

  @Test
  public void testGroupByWithLocalFilter()
  {
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT TIMESTAMP '2001-01-03 00:00:00' AS __time, cnt, SUM(cnt) AS sum_cnt FROM "
                + external() + " WHERE dim1 IN ('2', 'abc') GROUP BY cnt PARTITIONED BY DAY")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("sum_cnt", ColumnType.LONG).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, 2L}))
        .verifyResults();
    final List<JsonNode> scans = requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).toList();
    Assertions.assertEquals(2, scans.size());
    Assertions.assertTrue(scans.stream().allMatch(r -> r.path("filter").isMissingNode()));
  }
  @Test
  public void testBasicAuthenticationAndDiscoveredSchemaWithBoundPassword()
  {
    expectedAuthorization = "Basic cmVhZGVyOnNlY3JldA==";
    final String remote = "TABLE(REMOTE(endpoint => 'http://localhost:" + server.getAddress().getPort()
                          + "/druid/v2/', dataSource => 'foo', authType => 'basic', username => 'reader', password => ?,"
                          + " splitDurationMillis => 31536000000))";
    testIngestQuery()
        .setSql("INSERT INTO foo1 SELECT * FROM " + remote + " WHERE dim1 = 'abc' PARTITIONED BY DAY")
        .setDynamicParameters(List.of(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "secret")))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(RowSignature.builder().add("__time", ColumnType.LONG)
                                             .add("cnt", ColumnType.LONG).add("dim1", ColumnType.STRING).build())
        .setExpectedResultRows(List.<Object[]>of(new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc"}))
        .verifyResults();
    Assertions.assertTrue(requests.stream().anyMatch(r -> "segmentMetadata".equals(r.path("queryType").asText())));
    Assertions.assertEquals(2, requests.stream().filter(r -> "scan".equals(r.path("queryType").asText())).count());
  }

}
