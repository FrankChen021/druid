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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/// Verifies that EXPLAIN shows the remote Druid SQL that runs on the source cluster and the target-side remainder.
public class CalciteRemoteDruidPushdownTest extends CalciteIngestionDmlTest
{
  private static final String REMOTE = """
      TABLE(DRUID(endpoint => 'https://source.example', dataSource => 'events'%s))
        EXTEND (__time BIGINT, a VARCHAR, m BIGINT)""";

  private static String remote(final String extraArguments)
  {
    return StringUtils.format(REMOTE, extraArguments);
  }

  /// Plans an INSERT whose SELECT is the format string applied to the remote table.
  private JsonNode explainInsert(final String select, final String extraArguments)
  {
    return explain(StringUtils.format("INSERT INTO dst %s PARTITIONED BY DAY", StringUtils.format(select, remote(extraArguments))));
  }

  private JsonNode explain(final String sql)
  {
    skipVectorize();
    final List<String> plans = new ArrayList<>();
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        ImmutableMap.of("sqlQueryId", "dummy"),
        Collections.emptyList(),
        "EXPLAIN PLAN FOR " + sql,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        Collections.emptyList(),
        (s, results) -> plans.add((String) results.results.get(0)[0])
    );
    didTest = true;
    try {
      return queryFramework().queryJsonMapper().readTree(plans.get(0)).get(0).get("query");
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static JsonNode remoteInputSource(final JsonNode dataSource)
  {
    final JsonNode inputSource = dataSource.path("inputSource");
    Assertions.assertEquals("remoteDruidSql", inputSource.path("type").asText(), dataSource.toString());
    return inputSource;
  }

  @Test
  public void testFilterAndProjectionArePushedDownAndSplittable()
  {
    final JsonNode query = explainInsert("SELECT __time, UPPER(a) AS ua, m * 2 AS m2 FROM %s WHERE m > 3", "");
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertEquals(
        """
        SELECT "__time", UPPER("a") AS "ua", "m" * 2 AS "m2"
        FROM (SELECT "__time", "a", "m"
        FROM "events"
        WHERE "__time" >= MILLIS_TO_TIMESTAMP(?) AND "__time" < MILLIS_TO_TIMESTAMP(?)) AS "t"
        WHERE "m" > 3""",
        inputSource.path("sql").asText()
    );
    Assertions.assertTrue(inputSource.path("timeRangeParameters").asBoolean());
    Assertions.assertEquals("msq-dart", inputSource.path("engine").asText());
    Assertions.assertEquals("[\"__time\"]", inputSource.path("timestampColumns").toString());
    // Nothing is left to evaluate on the target.
    Assertions.assertTrue(query.path("filter").isMissingNode(), query.toString());
    Assertions.assertTrue(query.path("virtualColumns").isEmpty(), query.toString());
  }

  @Test
  public void testAggregationIsPushedDownWithLiteralIntervals()
  {
    final JsonNode query = explainInsert(
        "SELECT TIME_FLOOR(__time, 'PT1H') AS __time, a, SUM(m) AS m FROM %s WHERE a <> 'z' GROUP BY 1, 2",
        ", intervals => ARRAY['2000-01-01/2000-01-02'], splitDuration => 'PT1H'"
    );
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertEquals(
        """
        SELECT TIME_FLOOR("__time", 'PT1H') AS "__time", "a", SUM("m") AS "m"
        FROM (SELECT "__time", "a", "m"
        FROM "events"
        WHERE "__time" >= MILLIS_TO_TIMESTAMP(946684800000) AND "__time" < MILLIS_TO_TIMESTAMP(946771200000)) AS "t"
        WHERE "a" <> 'z'
        GROUP BY TIME_FLOOR("__time", 'PT1H'), "a\"""",
        inputSource.path("sql").asText()
    );
    // Aggregates over disjoint time ranges cannot be concatenated, so the read is not split.
    Assertions.assertFalse(inputSource.path("timeRangeParameters").asBoolean());
    Assertions.assertTrue(inputSource.path("splitDuration").isMissingNode());
    Assertions.assertEquals("scan", query.path("queryType").asText());
  }

  @Test
  public void testJoinStaysOnTarget()
  {
    final JsonNode query = explainInsert(
        "SELECT r.__time, r.a, f.dim2 FROM %s r INNER JOIN foo f ON r.a = f.dim1 WHERE r.m > 1",
        ""
    );
    Assertions.assertEquals("join", query.path("dataSource").path("type").asText());
    final JsonNode inputSource = remoteInputSource(query.path("dataSource").path("left"));
    Assertions.assertTrue(inputSource.path("sql").asText().contains("WHERE \"m\" > 1"), inputSource.toString());
    Assertions.assertFalse(inputSource.path("sql").asText().contains("foo"), inputSource.toString());
  }

  @Test
  public void testLookupStaysOnTarget()
  {
    final JsonNode query = explainInsert("SELECT __time, LOOKUP(a, 'lookyloo') AS l FROM %s", "");
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertFalse(inputSource.path("sql").asText().contains("LOOKUP"), inputSource.toString());
    Assertions.assertTrue(query.toString().contains("lookyloo"), query.toString());
  }

  @Test
  public void testPasswordEnvVarKeepsSecretOutOfPlan()
  {
    final JsonNode query = explainInsert(
        "SELECT * FROM %s",
        ", authType => 'basic', username => 'reader', passwordEnvVar => 'SOURCE_PASSWORD'"
    );
    final JsonNode authentication = remoteInputSource(query.path("dataSource")).path("connection").path("authentication");
    Assertions.assertEquals(
        """
        {"type":"basic","username":"reader","password":{"type":"environment","variable":"SOURCE_PASSWORD"}}""",
        authentication.toString()
    );
  }

  @Test
  public void testKeywordColumnNamesAreQuoted()
  {
    final JsonNode query = explain(
        """
        INSERT INTO dst
        SELECT __time, "user", "value"
        FROM TABLE(DRUID(endpoint => 'https://source.example', dataSource => 'events'))
          EXTEND (__time BIGINT, "user" VARCHAR, "value" BIGINT, "date" VARCHAR)
        WHERE "user" <> 'bot'
        PARTITIONED BY DAY"""
    );
    Assertions.assertEquals(
        """
        SELECT "__time", "user", "value"
        FROM (SELECT "__time", "user", "value", "date"
        FROM "events"
        WHERE "__time" >= MILLIS_TO_TIMESTAMP(?) AND "__time" < MILLIS_TO_TIMESTAMP(?)) AS "t"
        WHERE "user" <> 'bot'""",
        remoteInputSource(query.path("dataSource")).path("sql").asText()
    );
  }

  @Test
  public void testCredentialsAreNotPartOfSourceSql()
  {
    final JsonNode query = explainInsert(
        "SELECT * FROM %s",
        ", authType => 'basic', username => 'reader', password => 'secret', engine => 'native'"
    );
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertEquals("native", inputSource.path("engine").asText());
    Assertions.assertEquals(
        """
        SELECT "__time", "a", "m"
        FROM "events"
        WHERE "__time" >= MILLIS_TO_TIMESTAMP(?) AND "__time" < MILLIS_TO_TIMESTAMP(?)""",
        inputSource.path("sql").asText()
    );
  }
}
