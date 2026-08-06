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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.junit5.ExpectedFailureExtension;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;

@ParameterizedClass
@MethodSource("data")
public class PartialGenericSegmentMergeTaskTest extends AbstractParallelIndexSupervisorTaskTest
{

  public static Iterable<?> data()
  {
    return Arrays.asList(
        GENERIC_PARTITION_LOCATION,
        DEEP_STORE_PARTITION_LOCATION
    );
  }

  private final PartitionLocation partitionLocation;

  private static final GenericPartitionLocation GENERIC_PARTITION_LOCATION = new GenericPartitionLocation(
      ParallelIndexTestingFactory.HOST,
      ParallelIndexTestingFactory.PORT,
      ParallelIndexTestingFactory.USE_HTTPS,
      ParallelIndexTestingFactory.SUBTASK_ID,
      ParallelIndexTestingFactory.INTERVAL,
      ParallelIndexTestingFactory.HASH_BASED_NUMBERED_SHARD_SPEC
  );

  private static final DeepStoragePartitionLocation DEEP_STORE_PARTITION_LOCATION = new DeepStoragePartitionLocation(
      ParallelIndexTestingFactory.SUBTASK_ID,
      ParallelIndexTestingFactory.INTERVAL,
      ParallelIndexTestingFactory.HASH_BASED_NUMBERED_SHARD_SPEC,
      ImmutableMap.of()
  );

  @RegisterExtension
  public ExpectedFailureExtension exception = ExpectedFailureExtension.none();

  private PartialGenericSegmentMergeTask target;
  private PartialSegmentMergeIOConfig ioConfig;
  private HashedPartitionsSpec partitionsSpec;

  public PartialGenericSegmentMergeTaskTest(final PartitionLocation partitionLocation)
  {
    // We don't need to emulate transient failures for this test.
    super(0.0, 0.0);
    this.partitionLocation = partitionLocation;
  }

  @BeforeEach
  public void setup()
  {
    ioConfig = new PartialSegmentMergeIOConfig(Collections.singletonList(partitionLocation));
    partitionsSpec = new HashedPartitionsSpec(
        null,
        1,
        Collections.emptyList()
    );
    PartialSegmentMergeIngestionSpec ingestionSpec = new PartialSegmentMergeIngestionSpec(
        ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS),
        ioConfig,
        TuningConfigBuilder.forParallelIndexTask()
                           .withForceGuaranteedRollup(true)
                           .withPartitionsSpec(partitionsSpec)
                           .build()
    );
    target = new PartialGenericSegmentMergeTask(
        ParallelIndexTestingFactory.AUTOMATIC_ID,
        ParallelIndexTestingFactory.GROUP_ID,
        ParallelIndexTestingFactory.TASK_RESOURCE,
        ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
        ParallelIndexTestingFactory.SUBTASK_SPEC_ID,
        ParallelIndexTestingFactory.NUM_ATTEMPTS,
        ingestionSpec,
        ParallelIndexTestingFactory.CONTEXT
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(getObjectMapper(), target);
  }

  @Test
  public void hasCorrectPrefixForAutomaticId()
  {
    String id = target.getId();
    JUnit5Assertions.assertTrue(id.startsWith(PartialGenericSegmentMergeTask.TYPE));
  }

  @Test
  public void requiresGranularitySpecInputIntervals()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Missing intervals in granularitySpec");

    new PartialGenericSegmentMergeTask(
        ParallelIndexTestingFactory.AUTOMATIC_ID,
        ParallelIndexTestingFactory.GROUP_ID,
        ParallelIndexTestingFactory.TASK_RESOURCE,
        ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
        ParallelIndexTestingFactory.SUBTASK_SPEC_ID,
        ParallelIndexTestingFactory.NUM_ATTEMPTS,
        new PartialSegmentMergeIngestionSpec(
            ParallelIndexTestingFactory.createDataSchema(null),
            ioConfig,
            TuningConfigBuilder.forParallelIndexTask()
                .withForceGuaranteedRollup(true)
                .withPartitionsSpec(partitionsSpec)
                .build()
        ),
        ParallelIndexTestingFactory.CONTEXT
    );
  }

  @Test
  public void testGetInputSourceResources()
  {
    JUnit5Assertions.assertTrue(target.getInputSourceResources().isEmpty());
  }
}
