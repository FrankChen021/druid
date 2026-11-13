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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Verifies that EXPLAIN shows the remote Druid SQL that runs on the source cluster and the target-side remainder.
 */
public class RemoteFunctionPushdownTest extends CalciteIngestionDmlTest
{
  private static final String REMOTE = """
      TABLE(REMOTE(endpoint => 'https://source.example', dataSource => 'events'%s))
        EXTEND (__time BIGINT, a VARCHAR, m BIGINT)
      """;

  private static String remote(final String extraArguments)
  {
    return StringUtils.format(REMOTE, extraArguments);
  }

  /**
   * Plans an INSERT whose SELECT is the format string applied to the remote table.
   */
  private JsonNode explainInsert(final String select, final String extraArguments)
  {
    final String query = StringUtils.format(select, remote(extraArguments));
    return explain(StringUtils.format("INSERT INTO dst %s PARTITIONED BY DAY", query));
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

  /**
   * Returns the nodes anywhere in {@code node} with the given {@code type}, in document order.
   */
  private static List<JsonNode> findByType(final JsonNode node, final String type)
  {
    final List<JsonNode> found = new ArrayList<>();
    if (type.equals(node.path("type").asText())) {
      found.add(node);
    }
    for (final JsonNode child : node) {
      found.addAll(findByType(child, type));
    }
    return found;
  }

  private static JsonNode remoteInputSource(final JsonNode dataSource)
  {
    final JsonNode inputSource = dataSource.path("inputSource");
    Assertions.assertEquals("remote", inputSource.path("type").asText(), dataSource.toString());
    return inputSource;
  }

  @Test
  public void testRenamedColumnsKeepTheirNames()
  {
    // Projections that only rename columns are flattened away, but the source must return the planned names.
    final String where = "\nFROM \"events\"\n"
                         + "WHERE TIMESTAMP_TO_MILLIS(\"__time\") >= ? AND TIMESTAMP_TO_MILLIS(\"__time\") < ?";
    Assertions.assertEquals(
        "SELECT \"__time\", MV_TO_ARRAY(\"a\") AS \"x\", CAST(\"m\" AS BIGINT) AS \"y\"" + where,
        remoteInputSource(explainInsert("SELECT __time, a AS x, m AS y FROM %s", "").path("dataSource"))
            .path("sql").asText()
    );
    Assertions.assertEquals(
        "SELECT \"__time\", CAST(\"m\" AS BIGINT) AS \"a\", MV_TO_ARRAY(\"a\") AS \"m\"" + where,
        remoteInputSource(explainInsert("SELECT __time, m AS a, a AS m FROM %s", "").path("dataSource"))
            .path("sql").asText()
    );
    // A read without string columns has no projection left after flattening, so one is added for the names.
    Assertions.assertEquals(
        "SELECT \"__time\", CAST(\"m\" AS BIGINT) AS \"y\"" + where,
        remoteInputSource(explainInsert("SELECT __time, m AS y FROM %s", "").path("dataSource")).path("sql").asText()
    );
  }

  @Test
  public void testHavingRefersToSourceColumns()
  {
    // Druid resolves a name in HAVING to a select alias before a column, so HAVING names the source columns in full.
    Assertions.assertEquals(
        """
        SELECT TIME_SHIFT("__time", 'PT1H', 1) AS "__time", "a", COUNT(*) AS "c"
        FROM "events"
        GROUP BY TIME_SHIFT("__time", 'PT1H', 1), "a"
        HAVING TIME_SHIFT("events"."__time", 'PT1H', 1) >= TIMESTAMP '2000-01-02 00:00:00.000'\
        """,
        remoteInputSource(
            explainInsert(
                "SELECT * FROM (SELECT TIME_SHIFT(__time, 'PT1H', 1) AS __time, a, COUNT(*) AS c FROM %s GROUP BY 1, 2)"
                + " WHERE __time >= TIMESTAMP '2000-01-02'",
                ""
            ).path("dataSource")
        ).path("sql").asText()
    );
    Assertions.assertTrue(
        remoteInputSource(
            explainInsert(
                "SELECT * FROM (SELECT TIME_FLOOR(__time, 'P1D') AS __time, a AS x, SUM(m) AS a FROM %s"
                + " GROUP BY 1, a) WHERE x = 'z'",
                ""
            ).path("dataSource")
        ).path("sql").asText().endsWith("HAVING \"events\".\"a\" = 'z'")
    );
  }

  @Test
  public void testFilterAboveStringExpressionIsPushedDown()
  {
    final JsonNode inputSource = remoteInputSource(
        explainInsert(
            "SELECT * FROM (SELECT __time, UPPER(a) AS ua, m FROM %s) WHERE m > 3 AND __time >= TIMESTAMP '2000-01-02'",
            ", splitDuration => 'P1D'"
        ).path("dataSource")
    );
    Assertions.assertTrue(
        inputSource.path("sql").asText()
                   .endsWith("AND CAST(\"m\" AS BIGINT) > 3 AND \"__time\" >= TIMESTAMP '2000-01-02 00:00:00.000'"),
        inputSource.toString()
    );
    Assertions.assertTrue(inputSource.path("intervals").get(0).asText().startsWith("2000-01-02T00:00:00.000Z/"));
  }

  @Test
  public void testFilterAndProjectionArePushedDownAndSplittable()
  {
    final JsonNode query = explainInsert("SELECT __time, a, m * 2 AS m2 FROM %s WHERE m > 3", "");
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertEquals(
        """
        SELECT "__time", MV_TO_ARRAY("a") AS "a", CAST("m" AS BIGINT) * 2 AS "m2"
        FROM "events"
        WHERE TIMESTAMP_TO_MILLIS("__time") >= ? AND TIMESTAMP_TO_MILLIS("__time") < ? AND CAST("m" AS BIGINT) > 3\
        """,
        inputSource.path("sql").asText()
    );
    Assertions.assertTrue(inputSource.path("timeRangeParameters").asBoolean());
    // The source plans aggregations with the target's effective settings, in a deterministic order.
    Assertions.assertEquals(
        List.of(
            "sqlCurrentTimestamp",
            "sqlTimeZone",
            "useApproximateCountDistinct",
            "useApproximateTopN",
            "useGroupingSetForExactDistinct"
        ),
        ImmutableList.copyOf(inputSource.path("context").fieldNames())
    );
    Assertions.assertTrue(inputSource.path("context").path("useApproximateCountDistinct").asBoolean());
    Assertions.assertEquals("[\"__time\"]", inputSource.path("timestampColumns").toString());
    // Nothing is left to evaluate on the target.
    Assertions.assertTrue(query.path("filter").isMissingNode(), query.toString());
    Assertions.assertTrue(query.path("virtualColumns").isEmpty(), query.toString());
  }

  @Test
  public void testStringExpressionsWithoutAggregationRunOnTarget()
  {
    // The source would return UPPER of a multi-value string as JSON-encoded text, so the column is read as an array
    // and the expression is evaluated on the target, while the filter below it is still pushed down.
    final JsonNode query = explainInsert("SELECT __time, UPPER(a) AS ua FROM %s WHERE m > 3", "");
    Assertions.assertEquals(
        """
        SELECT "__time", MV_TO_ARRAY("a") AS "a", CAST("m" AS BIGINT) AS "m"
        FROM "events"
        WHERE TIMESTAMP_TO_MILLIS("__time") >= ? AND TIMESTAMP_TO_MILLIS("__time") < ? AND CAST("m" AS BIGINT) > 3\
        """,
        remoteInputSource(query.path("dataSource")).path("sql").asText()
    );
    Assertions.assertTrue(query.path("virtualColumns").toString().contains("upper"), query.toString());
  }

  @Test
  public void testTimeFilterNarrowsSplitIntervals()
  {
    final String day = "SELECT * FROM %s WHERE __time >= TIMESTAMP '2000-01-02' AND __time < TIMESTAMP '2000-01-03'";
    // Without intervals, only the filtered day is split.
    Assertions.assertEquals(
        "[\"2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.000Z\"]",
        remoteInputSource(explainInsert(day, ", splitDuration => 'PT1H'").path("dataSource"))
            .path("intervals").toString()
    );
    // Requested intervals are intersected with the filtered range.
    Assertions.assertEquals(
        "[\"2000-01-02T00:00:00.000Z/2000-01-02T12:00:00.000Z\"]",
        remoteInputSource(
            explainInsert(day, ", intervals => ARRAY['1999/2000-01-02T12'], splitDuration => 'PT1H'")
                .path("dataSource")
        ).path("intervals").toString()
    );
    // Exclusive lower and inclusive upper bounds keep every matching millisecond.
    Assertions.assertEquals(
        "[\"2000-01-02T00:00:00.001Z/2000-01-03T00:00:00.001Z\"]",
        remoteInputSource(
            explainInsert(
                "SELECT * FROM %s WHERE __time > TIMESTAMP '2000-01-02' AND __time <= TIMESTAMP '2000-01-03'",
                ", splitDuration => 'PT1H'"
            ).path("dataSource")
        ).path("intervals").toString()
    );
    Assertions.assertEquals(
        "[\"2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.001Z\"]",
        remoteInputSource(
            explainInsert(
                "SELECT * FROM %s WHERE __time BETWEEN TIMESTAMP '2000-01-02' AND TIMESTAMP '2000-01-03'",
                ", splitDuration => 'PT1H'"
            ).path("dataSource")
        ).path("intervals").toString()
    );
    // A range outside the requested intervals requests nothing.
    Assertions.assertEquals(
        "[]",
        remoteInputSource(
            explainInsert(day, ", intervals => ARRAY['2001/2002'], splitDuration => 'PT1H'").path("dataSource")
        ).path("intervals").toString()
    );
    // A __time computed from another column does not bound the source's __time.
    Assertions.assertTrue(
        remoteInputSource(
            explainInsert(
                "SELECT * FROM (SELECT MILLIS_TO_TIMESTAMP(m) AS __time, a FROM %s)"
                + " WHERE __time >= TIMESTAMP '2000-01-02'",
                ", splitDuration => 'PT1H'"
            ).path("dataSource")
        ).path("intervals").isMissingNode()
    );
    // A disjunction does not bound the range, so it is read in full.
    Assertions.assertTrue(
        remoteInputSource(
            explainInsert(
                "SELECT * FROM %s WHERE __time < TIMESTAMP '2000-01-02' OR a = 'x'",
                ", splitDuration => 'PT1H'"
            ).path("dataSource")
        ).path("intervals").isMissingNode()
    );
  }

  @Test
  public void testAggregationIsPushedDownWithLiteralIntervals()
  {
    final JsonNode query = explainInsert(
        "SELECT TIME_FLOOR(__time, 'PT1H') AS __time, a, SUM(m) AS m FROM %s WHERE a <> 'z' GROUP BY 1, 2"
        + " HAVING SUM(m) > 1",
        ", intervals => ARRAY['2000-01-01/2000-01-02'], splitDuration => 'PT1H'"
    );
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertEquals(
        """
        SELECT TIME_FLOOR("__time", 'PT1H') AS "__time", "a", SUM(CAST("m" AS BIGINT)) AS "m"
        FROM "events"
        WHERE TIMESTAMP_TO_MILLIS("__time") >= 946684800000 AND TIMESTAMP_TO_MILLIS("__time") < 946771200000 AND "a" <> 'z'
        GROUP BY TIME_FLOOR("__time", 'PT1H'), "a"
        HAVING SUM(CAST("events"."m" AS BIGINT)) > 1\
        """,
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
    final String sql = inputSource.path("sql").asText();
    Assertions.assertTrue(sql.contains("CAST(\"m\" AS BIGINT) > 1"), sql);
    Assertions.assertFalse(sql.contains("foo"), sql);
  }

  @Test
  public void testScalarSubquerySingleValueStaysOnTarget()
  {
    final JsonNode query = explainInsert(
        "SELECT __time, dim1 FROM foo WHERE cnt = (SELECT m FROM %s WHERE a = 'x')",
        ""
    );
    final List<JsonNode> inputSources = findByType(query, "remote");
    Assertions.assertEquals(1, inputSources.size(), query.toString());
    final String sql = inputSources.get(0).path("sql").asText();
    Assertions.assertTrue(sql.contains("\"a\" = 'x'"), sql);
    Assertions.assertFalse(sql.contains("SINGLE_VALUE"), sql);
    // The aggregate runs on the target instead.
    Assertions.assertFalse(findByType(query, "singleValue").isEmpty(), query.toString());
  }

  @Test
  public void testLookupStaysOnTarget()
  {
    final JsonNode query = explainInsert("SELECT __time, LOOKUP(a, 'lookyloo') AS l FROM %s", "");
    final JsonNode inputSource = remoteInputSource(query.path("dataSource"));
    Assertions.assertFalse(inputSource.path("sql").asText().contains("LOOKUP"), inputSource.toString());
    final List<JsonNode> virtualColumns = findByType(query.path("virtualColumns"), "expression");
    Assertions.assertTrue(
        virtualColumns.stream().anyMatch(column -> column.path("expression").asText().contains("lookyloo")),
        query.toString()
    );
  }

  @Test
  public void testKeywordColumnNamesAreQuoted()
  {
    final JsonNode query = explain(
        """
        INSERT INTO dst
        SELECT __time, "user", "value"
        FROM TABLE(REMOTE(endpoint => 'https://source.example', dataSource => 'events'))
          EXTEND (__time BIGINT, "user" VARCHAR, "value" BIGINT, "date" VARCHAR)
        WHERE "user" <> 'bot'
        PARTITIONED BY DAY
        """
    );
    Assertions.assertEquals(
        """
        SELECT "__time", MV_TO_ARRAY("user") AS "user", CAST("value" AS BIGINT) AS "value"
        FROM "events"
        WHERE TIMESTAMP_TO_MILLIS("__time") >= ? AND TIMESTAMP_TO_MILLIS("__time") < ? AND "user" <> 'bot'\
        """,
        remoteInputSource(query.path("dataSource")).path("sql").asText()
    );
  }

  @Test
  public void testExternRemoteTableUsesDeclaredTypes()
  {
    final JsonNode query = explain(
        """
        INSERT INTO dst
        SELECT * FROM TABLE(EXTERN(
          '{"type":"remote","connection":{"endpoint":"https://source.example"},"dataSource":"events"}',
          'null',
          '[{"name":"__time","type":"long"},{"name":"m","type":"long"}]'
        ))
        PARTITIONED BY DAY
        """
    );
    final String sql = remoteInputSource(query.path("dataSource")).path("sql").asText();
    Assertions.assertTrue(sql.contains("CAST(\"m\" AS BIGINT)"), sql);
  }

  @Test
  public void testExternRemoteReadWithSqlIsReadAsItIs()
  {
    final String inputSource = """
        {"type":"remote","connection":{"endpoint":"https://source.example"},"dataSource":"events",\
        "sql":"SELECT \\"__time\\", \\"m\\" FROM \\"events\\"",\
        "signature":[{"name":"__time","type":"LONG"},{"name":"m","type":"LONG"}]}\
        """;
    final JsonNode query = explain(
        StringUtils.format(
            """
            INSERT INTO dst
            SELECT * FROM TABLE(EXTERN('%s', 'null', '[{"name":"__time","type":"long"},{"name":"m","type":"long"}]'))
            WHERE m > 1
            PARTITIONED BY DAY
            """,
            inputSource
        )
    );
    Assertions.assertEquals(
        "SELECT \"__time\", \"m\" FROM \"events\"",
        remoteInputSource(query.path("dataSource")).path("sql").asText()
    );
    Assertions.assertFalse(query.path("filter").isMissingNode(), query.toString());

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> explain(
            StringUtils.format(
                """
                INSERT INTO dst
                SELECT * FROM TABLE(EXTERN(
                  '%s',
                  'null',
                  '[{"name":"__time","type":"long"},{"name":"n","type":"long"}]'
                ))
                PARTITIONED BY DAY
                """,
                inputSource
            )
        )
    );
    Assertions.assertTrue(e.getMessage().contains("must declare its signature"), e.getMessage());

    // A planned aggregation need not return __time.
    final JsonNode aggregate = explain(
        """
        INSERT INTO dst
        SELECT TIME_PARSE('2000-01-01') AS __time, a, cnt FROM TABLE(EXTERN(
          '{"type":"remote","connection":{"endpoint":"https://source.example"},"dataSource":"events",\
        "sql":"SELECT \\"a\\", COUNT(*) AS \\"cnt\\" FROM \\"events\\" GROUP BY \\"a\\"",\
        "signature":[{"name":"a","type":"STRING"},{"name":"cnt","type":"LONG"}]}',
          'null',
          '[{"name":"a","type":"string"},{"name":"cnt","type":"long"}]'
        ))
        PARTITIONED BY DAY
        """
    );
    Assertions.assertTrue(
        remoteInputSource(aggregate.path("dataSource")).path("sql").asText().contains("GROUP BY"),
        aggregate.toString()
    );
  }

  @Test
  public void testExternRemoteTableRequiresSupportedColumns()
  {
    for (final String columns : List.of(
        "[{\"name\":\"m\",\"type\":\"long\"}]",
        "[{\"name\":\"__time\",\"type\":\"long\"},{\"name\":\"u\",\"type\":\"COMPLEX<hyperUnique>\"}]"
    )) {
      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> explain(
              StringUtils.format(
                  """
                  INSERT INTO dst
                  SELECT * FROM TABLE(EXTERN(
                    '{"type":"remote","connection":{"endpoint":"https://source.example"},"dataSource":"events"}',
                    'null',
                    '%s'
                  ))
                  PARTITIONED BY DAY
                  """,
                  columns
              )
          )
      );
      Assertions.assertTrue(e.getMessage().contains("A remote table"), e.getMessage());
    }
    didTest = true;
  }
}
