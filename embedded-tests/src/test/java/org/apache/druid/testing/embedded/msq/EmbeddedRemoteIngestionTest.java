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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ingests with {@code TABLE(REMOTE(...))} from a datasource of the same cluster, through its Broker's SQL API. The
 * source reads run on the real Dart engine (or the native engine), and the target is an ordinary MSQ ingestion task.
 */
public class EmbeddedRemoteIngestionTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker =
      new EmbeddedBroker().setServerMemory(200_000_000)
                          .addProperty("druid.msq.dart.controller.heapFraction", "0.5")
                          .addProperty("druid.query.default.context.maxConcurrentStages", "1");
  private final EmbeddedHistorical historical =
      new EmbeddedHistorical().setServerMemory(300_000_000)
                              .addProperty("druid.msq.dart.worker.heapFraction", "0.5")
                              .addProperty("druid.msq.dart.worker.concurrentQueries", "1");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "3");

  private EmbeddedMSQApis msqApis;
  /**
   * Loaded once and read by every test, which each write their own target datasource.
   */
  private String sourceDataSource;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(historical);
  }

  @BeforeAll
  public void loadSourceData()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    sourceDataSource = EmbeddedClusterApis.createTestDatasourceName();
    runTask(StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        sourceDataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    ));
    cluster.callApi().waitForAllSegmentsToBeAvailable(sourceDataSource, coordinator, broker);
  }

  private String remote(final String extraArguments)
  {
    return StringUtils.format(
        "TABLE(REMOTE(endpoint => '%s', dataSource => '%s'%s))",
        broker.bindings().selfNode().getUriToUse(),
        sourceDataSource,
        extraArguments
    );
  }

  private long dartQueries()
  {
    return broker.latchableEmitter()
                 .getMetricEvents("sqlQuery/time")
                 .stream()
                 .filter(event -> "msq-dart".equals(event.getUserDims().get("engine")))
                 .count();
  }

  /**
   * Waits until the Broker has reported at least {@code expected} Dart queries, since it reports a query after
   * streaming it.
   */
  private long waitForDartQueries(final long expected)
  {
    final long deadline = System.currentTimeMillis() + 10_000;
    long count = dartQueries();
    while (count < expected && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      count = dartQueries();
    }
    return count;
  }

  private void runTask(final String sql)
  {
    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
  }

  private void ingestAndVerify(final String insertSql, final String verifySql, final String expectedCsv)
  {
    runTask(insertSql);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery(verifySql, dataSource, expectedCsv);
  }

  @Test
  public void testFilteredCopyWithDiscoveredSchemaAndTimeSplitsOnDart()
  {
    final long dartQueriesBefore = dartQueries();
    ingestAndVerify(
        StringUtils.format(
            "INSERT INTO %s SELECT __time, page, added FROM %s WHERE namespace = 'article' PARTITIONED BY DAY",
            dataSource,
            remote(", splitDuration => 'PT2H'")
        ),
        "SELECT __time, page, added FROM %s ORDER BY __time",
        """
        2013-08-31T01:02:33.000Z,Gypsy Danger,57
        2013-08-31T07:11:21.000Z,Cherno Alpha,123\
        """
    );
    // The rows span 01:02 to 07:11 UTC, so the read is split into the 2-hour ranges starting at 00:00, 02:00, 04:00,
    // and 06:00, each read by its own Dart query on the source.
    Assertions.assertEquals(4, waitForDartQueries(dartQueriesBefore + 4) - dartQueriesBefore);
  }

  @Test
  public void testAggregationOnDart()
  {
    ingestAndVerify(
        StringUtils.format(
            """
            INSERT INTO %s
            SELECT TIME_FLOOR(__time, 'P1D') AS __time, namespace, SUM(added) AS added, COUNT(*) AS cnt
            FROM %s EXTEND (__time BIGINT, namespace VARCHAR, added BIGINT)
            GROUP BY 1, 2
            PARTITIONED BY DAY
            """,
            dataSource,
            remote("")
        ),
        "SELECT __time, namespace, SUM(added), SUM(cnt) FROM %s GROUP BY 1, 2",
        """
        2013-08-31T00:00:00.000Z,article,180,2
        2013-08-31T00:00:00.000Z,wikipedia,459,1\
        """
    );
  }

  @Test
  public void testCopyOnNativeEngine()
  {
    ingestAndVerify(
        StringUtils.format(
            "INSERT INTO %s SELECT __time, page, namespace FROM %s WHERE delta > 0 PARTITIONED BY DAY",
            dataSource,
            remote(", engine => 'native'")
        ),
        "SELECT __time, page, namespace FROM %s ORDER BY __time",
        """
        2013-08-31T03:32:45.000Z,Striker Eureka,wikipedia
        2013-08-31T07:11:21.000Z,Cherno Alpha,article\
        """
    );
  }
}
