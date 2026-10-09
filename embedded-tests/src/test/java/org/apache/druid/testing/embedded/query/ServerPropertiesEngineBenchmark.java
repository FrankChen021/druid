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

package org.apache.druid.testing.embedded.query;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manual end-to-end benchmark for the distributed Dart path over a large {@code sys.server_properties} table. The
 * class name intentionally does not end in {@code Test}, so it is not part of the normal embedded-test suite. Run it
 * explicitly with
 * {@code -Ddruid.test.benchmark=true -Dtest=ServerPropertiesEngineBenchmark}.
 */
@Tag("perf")
@EnabledIfSystemProperty(named = "druid.test.benchmark", matches = "true")
public class ServerPropertiesEngineBenchmark extends EmbeddedClusterTestBase
{
  private static final Logger LOG = new Logger(ServerPropertiesEngineBenchmark.class);
  private static final int PROPERTY_COUNT = 100_001;
  private static final int WARMUP_ITERATIONS = 3;
  private static final int MEASUREMENT_ITERATIONS = 10;
  private static final String PROPERTY_PREFIX = "benchmark.property.";
  private static final String AGGREGATE_SQL_FORMAT =
      "SELECT COUNT(*), COUNT(DISTINCT property) "
      + "FROM sys.server_properties WHERE property LIKE '" + PROPERTY_PREFIX + "%%'";
  private static final String AGGREGATE_SQL = StringUtils.replace(AGGREGATE_SQL_FORMAT, "%%", "%");
  private static final String EXACT_AGGREGATE_SQL = "SET useApproximateCountDistinct = false; " + AGGREGATE_SQL;
  private static final String SCAN_SQL = "SELECT * FROM sys.server_properties";
  private static final String ORDERED_SCAN_SQL = SCAN_SQL + " ORDER BY property, server";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .setServerMemory(1_000_000_000L)
      .setServerDirectMemory(1_000_000_000L)
      .addProperty("druid.msq.dart.controller.heapFraction", "0.5");
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedHistorical historical = makeHistorical();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(historical);
  }

  /** Compare one-stage execution and two-stage pipelining on the same 100K-row aggregate and scan workloads. */
  @Test
  public void benchmarkDart()
  {
    final Map<Integer, Map<String, Object>> contexts = Map.of(
        1, Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME, "maxConcurrentStages", 1),
        2, Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME, "maxConcurrentStages", 2)
    );
    final int scanRows = runScanAndCountRows(SCAN_SQL, contexts.get(1));
    Assertions.assertTrue(scanRows > PROPERTY_COUNT);

    for (final String sql : List.of(AGGREGATE_SQL, EXACT_AGGREGATE_SQL, SCAN_SQL, ORDERED_SCAN_SQL)) {
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        for (final int stages : List.of(1, 2)) {
          measureWorkload(sql, contexts.get(stages), scanRows);
        }
      }
      final Map<Integer, List<Long>> samples = Map.of(
          1, new ArrayList<>(MEASUREMENT_ITERATIONS),
          2, new ArrayList<>(MEASUREMENT_ITERATIONS)
      );
      // Alternate which setting runs first so compilation and cache warming do not consistently favor one setting.
      for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
        for (final int stages : i % 2 == 0 ? List.of(1, 2) : List.of(2, 1)) {
          samples.get(stages).add(measureWorkload(sql, contexts.get(stages), scanRows));
        }
      }
      for (final int stages : List.of(1, 2)) {
        final boolean aggregate = sql.endsWith(AGGREGATE_SQL);
        logResults(
            aggregate ? "aggregate" : "scan",
            sql,
            aggregate ? PROPERTY_COUNT : scanRows,
            stages,
            samples.get(stages)
        );
      }
    }
  }

  private long measureWorkload(final String sql, final Map<String, Object> context, final int scanRows)
  {
    return sql.endsWith(AGGREGATE_SQL) ? measureAggregate(sql, context) : measureScan(sql, context, scanRows);
  }

  private static void logResults(
      final String workload,
      final String sql,
      final int rowCount,
      final int maxConcurrentStages,
      final List<Long> dartNanos
  )
  {
    final BenchmarkResult dartResult = BenchmarkResult.from(dartNanos);
    LOG.info(
        "sys.server_properties Dart benchmark: workload[%s], rows[%,d], maxConcurrentStages[%d], query[%s], result[%s]",
        workload,
        rowCount,
        maxConcurrentStages,
        sql,
        dartResult
    );
  }

  private long measureAggregate(final String sql, final Map<String, Object> queryContext)
  {
    final long start = System.nanoTime();
    runAggregateAndVerify(EXACT_AGGREGATE_SQL.equals(sql), queryContext);
    return System.nanoTime() - start;
  }

  private void runAggregateAndVerify(final boolean exact, final Map<String, Object> queryContext)
  {
    final String sql = exact
                       ? "SET useApproximateCountDistinct = false; " + AGGREGATE_SQL_FORMAT
                       : AGGREGATE_SQL_FORMAT;
    final String[] result = cluster.runSql(sql, queryContext).split(",");
    final Integer rowCount = Ints.tryParse(result[0]);
    final Integer distinctPropertyCount = Ints.tryParse(result[1]);
    Assertions.assertNotNull(rowCount);
    Assertions.assertNotNull(distinctPropertyCount);
    Assertions.assertEquals(PROPERTY_COUNT, rowCount);
    if (exact) {
      Assertions.assertEquals(PROPERTY_COUNT, distinctPropertyCount);
    } else {
      Assertions.assertTrue(distinctPropertyCount > PROPERTY_COUNT * 0.9);
      Assertions.assertTrue(distinctPropertyCount < PROPERTY_COUNT * 1.1);
    }
  }

  private long measureScan(final String sql, final Map<String, Object> queryContext, final int expectedRows)
  {
    final long start = System.nanoTime();
    Assertions.assertEquals(expectedRows, runScanAndCountRows(sql, queryContext));
    return System.nanoTime() - start;
  }

  private int runScanAndCountRows(final String sql, final Map<String, Object> queryContext)
  {
    return Math.toIntExact(cluster.runSql(sql, queryContext).lines().count());
  }

  private static EmbeddedHistorical makeHistorical()
  {
    final EmbeddedHistorical historical = new EmbeddedHistorical().setServerMemory(1_000_000_000L);
    for (int i = 0; i < PROPERTY_COUNT; i++) {
      historical.addProperty(PROPERTY_PREFIX + i, "value-" + i);
    }
    return historical;
  }

  private record BenchmarkResult(double meanMillis, double medianMillis, double minMillis, double maxMillis)
  {
    private static BenchmarkResult from(final List<Long> samples)
    {
      final List<Long> sorted = new ArrayList<>(samples);
      Collections.sort(sorted);
      final double nanosPerMillisecond = TimeUnit.MILLISECONDS.toNanos(1);
      return new BenchmarkResult(
          samples.stream().mapToLong(Long::longValue).average().orElseThrow() / nanosPerMillisecond,
          sorted.get(sorted.size() / 2) / nanosPerMillisecond,
          sorted.get(0) / nanosPerMillisecond,
          sorted.get(sorted.size() - 1) / nanosPerMillisecond
      );
    }

    @Override
    public String toString()
    {
      return StringUtils.format(
          "mean=%.2f ms, median=%.2f ms, min=%.2f ms, max=%.2f ms",
          meanMillis,
          medianMillis,
          minMillis,
          maxMillis
      );
    }
  }
}
