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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaIndexFaultToleranceTest extends StreamIndexFaultToleranceTest
{
  private final KafkaResource kafkaResource = new KafkaResource();
  private boolean coordinatorRestartComplete;

  @BeforeEach
  public void resetState()
  {
    coordinatorRestartComplete = false;
  }

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaResource;
  }

  @Override
  protected SupervisorSpec createSupervisorSpec(String dataSource, String topic)
  {
    return createKafkaSupervisor(kafkaResource).withId("supe_" + dataSource).build(dataSource, topic);
  }

  @Test
  public void test_supervisorRecovers_afterCoordinatorRestart() throws Exception
  {
    publishRecords_whileCoordinatorRestarts();
    coordinatorRestartComplete = true;
  }

  @Override
  protected void waitUntilPublishedRecordsAreIngested(int expectedRowCount)
  {
    if (!coordinatorRestartComplete) {
      super.waitUntilPublishedRecordsAreIngested(expectedRowCount);
      return;
    }

    // Publication can commit while cancellation prevents the success callback from emitting ingest/rows/published.
    // After the Coordinator restart, verify committed rows so teardown does not depend on that metric being delivered.
    final long publishedRows = cluster.callApi().waitForResult(
        () -> IndexTaskUtils.getTotalRowCount(cluster.callApi().getVisibleUsedSegments(dataSource, overlord)),
        rows -> rows >= expectedRowCount
    ).withTimeoutMillis(240_000).go();
    Assertions.assertEquals(expectedRowCount, publishedRows);
  }

  @Override
  protected void verifyRowCount(int expectedRowCount)
  {
    if (!coordinatorRestartComplete) {
      super.verifyRowCount(expectedRowCount);
      return;
    }

    // Broker visibility can lag committed metadata while segments are being handed off.
    cluster.callApi().waitForAllSegmentsToBeQueryable(dataSource, coordinator, 240_000);
    final String expected = String.valueOf(expectedRowCount);
    final String actual = cluster.callApi().waitForResult(
        () -> cluster.runSql("SELECT COUNT(*) FROM %s", dataSource),
        expected::equals
    ).withTimeoutMillis(240_000).go();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_supervisorRecords_afterHistoricalRestart() throws Exception
  {
    publishRecords_whileHistoricalRestarts();
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterOverlordRestart(boolean useTransactions) throws Exception
  {
    publishRecords_whileOverlordRestarts(useTransactions);
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterSuspendResume(boolean useTransactions)
  {
    publishRecords_andSuspendResumeSupervisor(useTransactions);
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterChangeInTopicPartitions(boolean useTransactions)
  {
    publishRecords_andIncreaseTopicPartitions(useTransactions);
  }
}
