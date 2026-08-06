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

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.test.JUnitAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ControllerImplTest
{

  @Mock
  private StageDefinition stageDefinition;
  @Mock
  private ClusterBy clusterBy;
  private AutoCloseable mocks;


  @BeforeEach
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);
    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();
    doReturn(clusterBy).when(stageDefinition).getClusterBy();

  }

  @Test
  public void test_performSegmentPublish_ok() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null, null, null);

    final TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(action)).thenReturn(SegmentPublishResult.ok(Collections.emptySet()));

    // All OK.
    ControllerImpl.performSegmentPublish(taskActionClient, action);
    verify(taskActionClient).submit(action);
  }

  @Test
  public void test_performSegmentPublish_publishFail() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null, null, null);

    final TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(action)).thenReturn(SegmentPublishResult.fail("oops"));

    final MSQException e = JUnitAssertions.assertThrows(
        MSQException.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    JUnitAssertions.assertEquals(InsertLockPreemptedFault.instance(), e.getFault());
    verify(taskActionClient).submit(action);
  }

  @Test
  public void test_performSegmentPublish_publishException() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null, null, null);

    final TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(action)).thenThrow(new ISE("oops"));

    final ISE e = JUnitAssertions.assertThrows(
        ISE.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    JUnitAssertions.assertEquals("oops", e.getMessage());
    verify(taskActionClient).submit(action);
  }

  @Test
  public void test_performSegmentPublish_publishLockPreemptedException() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null, null, null);

    final TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(action)).thenThrow(new ISE("are not covered by locks"));

    final MSQException e = JUnitAssertions.assertThrows(
        MSQException.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    JUnitAssertions.assertEquals(InsertLockPreemptedFault.instance(), e.getFault());
    verify(taskActionClient).submit(action);
  }


  @Test
  public void test_belowThresholds_ShouldBeParallel()
  {
    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count below threshold
    doReturn(1).when(stageDefinition).getMaxWorkerCount();

    JUnitAssertions.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );
  }


  @Test
  public void test_noClusterByColumns_shouldBeParallel()
  {

    // Cluster by bucket count 0
    doReturn(ClusterBy.none()).when(stageDefinition).getClusterBy();

    // Worker count above threshold
    doReturn((int) Limits.MAX_WORKERS_FOR_PARALLEL_MERGE + 1).when(stageDefinition).getMaxWorkerCount();

    JUnitAssertions.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );

  }

  @Test
  public void test_numWorkersAboveThreshold_shouldBeSequential()
  {
    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count above threshold
    doReturn((int) Limits.MAX_WORKERS_FOR_PARALLEL_MERGE + 1).when(stageDefinition).getMaxWorkerCount();

    JUnitAssertions.assertEquals(
        ClusterStatisticsMergeMode.SEQUENTIAL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );

  }

  @Test
  public void test_mode_should_not_change()
  {

    JUnitAssertions.assertEquals(
        ClusterStatisticsMergeMode.SEQUENTIAL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(null, ClusterStatisticsMergeMode.SEQUENTIAL)
    );
    JUnitAssertions.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(null, ClusterStatisticsMergeMode.PARALLEL)
    );
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mocks.close();
  }
}
