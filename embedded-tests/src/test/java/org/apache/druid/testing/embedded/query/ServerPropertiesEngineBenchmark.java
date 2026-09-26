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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manual end-to-end benchmark comparing the traditional Bindable path of the native SQL engine and the distributed
 * Dart path for a large
 * {@code sys.server_properties} table. The class name intentionally does not end in {@code Test}, so it is not part
 * of the normal embedded-test suite. Run it explicitly with {@code -Dtest=ServerPropertiesEngineBenchmark}.
 */
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
  private static final String AGGREGATE_SQL = AGGREGATE_SQL_FORMAT.replace("%%", "%");
  private static final String SCAN_SQL = "SELECT * FROM sys.server_properties";

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

  @Test
  public void benchmarkNativeAndDart()
  {
    final Map<String, Object> nativeContext = queryContext(NativeSqlEngine.NAME);
    final Map<String, Object> dartContext = queryContext(DartSqlEngine.NAME);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runAggregateAndVerify(nativeContext);
      runAggregateAndVerify(dartContext);
    }

    final List<Long> nativeNanos = new ArrayList<>(MEASUREMENT_ITERATIONS);
    final List<Long> dartNanos = new ArrayList<>(MEASUREMENT_ITERATIONS);
    for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
      // Alternate the order to reduce bias from GC and other process-wide activity.
      if ((i & 1) == 0) {
        nativeNanos.add(measureAggregate(nativeContext));
        dartNanos.add(measureAggregate(dartContext));
      } else {
        dartNanos.add(measureAggregate(dartContext));
        nativeNanos.add(measureAggregate(nativeContext));
      }
    }

    logResults("aggregate", AGGREGATE_SQL, PROPERTY_COUNT, nativeNanos, dartNanos);

    final int nativeScanRows = runScanAndCountRows(nativeContext);
    final int dartScanRows = runScanAndCountRows(dartContext);
    Assertions.assertTrue(nativeScanRows > PROPERTY_COUNT);
    Assertions.assertTrue(dartScanRows > PROPERTY_COUNT);

    nativeNanos.clear();
    dartNanos.clear();
    for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
      if ((i & 1) == 0) {
        nativeNanos.add(measureScan(nativeContext, nativeScanRows));
        dartNanos.add(measureScan(dartContext, dartScanRows));
      } else {
        dartNanos.add(measureScan(dartContext, dartScanRows));
        nativeNanos.add(measureScan(nativeContext, nativeScanRows));
      }
    }

    logScanResults(nativeScanRows, dartScanRows, nativeNanos, dartNanos);
  }

  private static void logResults(
      final String workload,
      final String sql,
      final int rowCount,
      final List<Long> nativeNanos,
      final List<Long> dartNanos
  )
  {
    final BenchmarkResult nativeResult = BenchmarkResult.from(nativeNanos);
    final BenchmarkResult dartResult = BenchmarkResult.from(dartNanos);
    LOG.info(
        "sys.server_properties benchmark: workload[%s], rows[%,d], query[%s], native[%s], dart[%s], "
        + "dart/native median ratio[%.2f]",
        workload,
        rowCount,
        sql,
        nativeResult,
        dartResult,
        dartResult.medianMillis() / nativeResult.medianMillis()
    );
  }

  private static void logScanResults(
      final int nativeRowCount,
      final int dartRowCount,
      final List<Long> nativeNanos,
      final List<Long> dartNanos
  )
  {
    final BenchmarkResult nativeResult = BenchmarkResult.from(nativeNanos);
    final BenchmarkResult dartResult = BenchmarkResult.from(dartNanos);
    LOG.info(
        "sys.server_properties benchmark: workload[full scan], native rows[%,d], dart rows[%,d], query[%s], "
        + "native[%s], dart[%s], dart/native median ratio[%.2f]",
        nativeRowCount,
        dartRowCount,
        SCAN_SQL,
        nativeResult,
        dartResult,
        dartResult.medianMillis() / nativeResult.medianMillis()
    );
  }

  private long measureAggregate(final Map<String, Object> queryContext)
  {
    final long start = System.nanoTime();
    runAggregateAndVerify(queryContext);
    return System.nanoTime() - start;
  }

  private void runAggregateAndVerify(final Map<String, Object> queryContext)
  {
    final String[] result = cluster.runSql(AGGREGATE_SQL_FORMAT, queryContext).split(",");
    Assertions.assertEquals(PROPERTY_COUNT, Integer.parseInt(result[0]));
    Assertions.assertTrue(Integer.parseInt(result[1]) > PROPERTY_COUNT * 0.9);
    Assertions.assertTrue(Integer.parseInt(result[1]) < PROPERTY_COUNT * 1.1);
  }

  private long measureScan(final Map<String, Object> queryContext, final int expectedRows)
  {
    final long start = System.nanoTime();
    Assertions.assertEquals(expectedRows, runScanAndCountRows(queryContext));
    return System.nanoTime() - start;
  }

  private int runScanAndCountRows(final Map<String, Object> queryContext)
  {
    return Math.toIntExact(cluster.runSql(SCAN_SQL, queryContext).lines().count());
  }

  private static Map<String, Object> queryContext(final String engine)
  {
    return Map.of(QueryContexts.ENGINE, engine);
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
      return String.format(
          "mean=%.2f ms, median=%.2f ms, min=%.2f ms, max=%.2f ms",
          meanMillis,
          medianMillis,
          minMillis,
          maxMillis
      );
    }
  }
}
