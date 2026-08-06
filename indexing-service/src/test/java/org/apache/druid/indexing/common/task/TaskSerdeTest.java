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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.junit5.ExpectedFailureExtension;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.joda.time.Period;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

public class TaskSerdeTest
{
  private final ObjectMapper jsonMapper;
  private final IndexSpec indexSpec = IndexSpec.getDefault();

  @RegisterExtension
  public ExpectedFailureExtension thrown = ExpectedFailureExtension.none();

  public TaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTuningConfig.class, "index")
    );
  }

  @Test
  public void testIndexTaskIOConfigDefaults() throws Exception
  {
    final IndexTask.IndexIOConfig ioConfig = jsonMapper.readValue(
        "{\"type\":\"index\",\"inputSource\":{\"type\":\"noop\"},\"inputFormat\":{\"type\":\"noop\"}}",
        IndexTask.IndexIOConfig.class
    );

    JUnit5Assertions.assertEquals(false, ioConfig.isAppendToExisting());
    JUnit5Assertions.assertEquals(false, ioConfig.isDropExisting());
  }

  @Test
  public void testIndexTaskTuningConfigDefaults() throws Exception
  {
    final IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertFalse(tuningConfig.isReportParseExceptions());
    JUnit5Assertions.assertEquals(IndexSpec.getDefault(), tuningConfig.getIndexSpec());
    JUnit5Assertions.assertEquals(new Period(Integer.MAX_VALUE), tuningConfig.getIntermediatePersistPeriod());
    JUnit5Assertions.assertEquals(0, tuningConfig.getMaxPendingPersists());
    JUnit5Assertions.assertEquals(1000000, tuningConfig.getMaxRowsInMemory());
    JUnit5Assertions.assertNull(tuningConfig.getNumShards());
    JUnit5Assertions.assertNull(tuningConfig.getMaxRowsPerSegment());
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeOrNumShards() throws Exception
  {
    IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertNull(tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertNull(tuningConfig.getMaxRowsPerSegment());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"maxRowsPerSegment\":10}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertNull(tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"numShards\":10, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertNull(tuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertEquals(10, (int) tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":10, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertNull(tuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertEquals(10, (int) tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":-1}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertNull(tuningConfig.getNumShards());
    JUnit5Assertions.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":-1, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    JUnit5Assertions.assertNull(tuningConfig.getNumShards());
    JUnit5Assertions.assertNotNull(tuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, tuningConfig.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeAndNumShards() throws Exception
  {
    thrown.expectCause(IllegalArgumentException.class);

    jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":10, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );
  }

  @Test
  public void testTaskResourceValid() throws Exception
  {
    TaskResource actual = jsonMapper.readValue(
        "{\"availabilityGroup\":\"index_xxx_mmm\", \"requiredCapacity\":1}",
        TaskResource.class
    );
    JUnit5Assertions.assertNotNull(actual);
    JUnit5Assertions.assertNotNull(actual.getAvailabilityGroup());
    JUnit5Assertions.assertTrue(actual.getRequiredCapacity() > 0);
  }

  @Test
  public void testTaskResourceWithNullAvailabilityGroupShouldFail() throws Exception
  {
    thrown.expectCause(NullPointerException.class);
    jsonMapper.readValue(
        "{\"availabilityGroup\":null, \"requiredCapacity\":10}",
        TaskResource.class
    );
  }

  @Test
  public void testTaskResourceWithZeroRequiredCapacityShouldFail() throws Exception
  {
    thrown.expectCause(NullPointerException.class);
    jsonMapper.readValue(
        "{\"availabilityGroup\":null, \"requiredCapacity\":0}",
        TaskResource.class
    );
  }

  @Test
  public void testTaskResourceWithNegativeRequiredCapacityShouldFail() throws Exception
  {
    thrown.expectCause(NullPointerException.class);
    jsonMapper.readValue(
        "{\"availabilityGroup\":null, \"requiredCapacity\":-1}",
        TaskResource.class
    );
  }

  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(TimestampSpec.DEFAULT)
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new LocalInputSource(new File("lol"), "rofl"), new NoopInputFormat(), true, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsInMemory(10)
                               .withPartitionsSpec(new DynamicPartitionsSpec(10000, null))
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .withForceGuaranteedRollup(false)
                               .withAwaitSegmentAvailabilityTimeoutMillis(1L)
                               .build()
        ),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    JUnit5Assertions.assertEquals(task.getDataSource(), "foo");

    JUnit5Assertions.assertEquals(task.getId(), task2.getId());
    JUnit5Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    JUnit5Assertions.assertEquals(task.getDataSource(), task2.getDataSource());

    IndexTask.IndexIOConfig taskIoConfig = task.getIngestionSchema().getIOConfig();
    IndexTask.IndexIOConfig task2IoConfig = task2.getIngestionSchema().getIOConfig();

    JUnit5Assertions.assertTrue(taskIoConfig.getInputSource() instanceof LocalInputSource);
    JUnit5Assertions.assertTrue(task2IoConfig.getInputSource() instanceof LocalInputSource);
    JUnit5Assertions.assertEquals(taskIoConfig.isAppendToExisting(), task2IoConfig.isAppendToExisting());
    JUnit5Assertions.assertEquals(taskIoConfig.isDropExisting(), task2IoConfig.isDropExisting());

    IndexTask.IndexTuningConfig taskTuningConfig = task.getIngestionSchema().getTuningConfig();
    IndexTask.IndexTuningConfig task2TuningConfig = task2.getIngestionSchema().getTuningConfig();

    JUnit5Assertions.assertEquals(taskTuningConfig.getBasePersistDirectory(), task2TuningConfig.getBasePersistDirectory());
    JUnit5Assertions.assertEquals(taskTuningConfig.getIndexSpec(), task2TuningConfig.getIndexSpec());
    JUnit5Assertions.assertEquals(
        taskTuningConfig.getIntermediatePersistPeriod(),
        task2TuningConfig.getIntermediatePersistPeriod()
    );
    JUnit5Assertions.assertEquals(taskTuningConfig.getMaxPendingPersists(), task2TuningConfig.getMaxPendingPersists());
    JUnit5Assertions.assertEquals(taskTuningConfig.getMaxRowsInMemory(), task2TuningConfig.getMaxRowsInMemory());
    JUnit5Assertions.assertEquals(taskTuningConfig.getNumShards(), task2TuningConfig.getNumShards());
    JUnit5Assertions.assertEquals(taskTuningConfig.getMaxRowsPerSegment(), task2TuningConfig.getMaxRowsPerSegment());
    JUnit5Assertions.assertEquals(taskTuningConfig.isReportParseExceptions(), task2TuningConfig.isReportParseExceptions());
    JUnit5Assertions.assertEquals(taskTuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(), task2TuningConfig.getAwaitSegmentAvailabilityTimeoutMillis());
  }

  @Test
  public void testIndexTaskwithResourceSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        new TaskResource("rofl", 2),
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(TimestampSpec.DEFAULT)
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new LocalInputSource(new File("lol"), "rofl"), new NoopInputFormat(), true, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsInMemory(10)
                               .withForceGuaranteedRollup(false)
                               .withPartitionsSpec(new DynamicPartitionsSpec(10000, null))
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .build()
        ),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    JUnit5Assertions.assertEquals(task.getDataSource(), "foo");

    JUnit5Assertions.assertEquals(task.getId(), task2.getId());
    JUnit5Assertions.assertEquals(2, task.getTaskResource().getRequiredCapacity());
    JUnit5Assertions.assertEquals(task.getTaskResource().getAvailabilityGroup(), "rofl");
    JUnit5Assertions.assertEquals(task.getTaskResource().getRequiredCapacity(), task2.getTaskResource().getRequiredCapacity());
    JUnit5Assertions.assertEquals(task.getTaskResource().getAvailabilityGroup(), task2.getTaskResource().getAvailabilityGroup());
    JUnit5Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    JUnit5Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    JUnit5Assertions.assertTrue(task.getIngestionSchema().getIOConfig().getInputSource() instanceof LocalInputSource);
    JUnit5Assertions.assertTrue(task2.getIngestionSchema().getIOConfig().getInputSource() instanceof LocalInputSource);
  }

  @Test
  public void testArchiveTaskSerde() throws Exception
  {
    final ArchiveTask task = new ArchiveTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final ArchiveTask task2 = (ArchiveTask) jsonMapper.readValue(json, Task.class);

    JUnit5Assertions.assertEquals(task.getDataSource(), "foo");
    JUnit5Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    JUnit5Assertions.assertEquals(task.getId(), task2.getId());
    JUnit5Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    JUnit5Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    JUnit5Assertions.assertEquals(task.getInterval(), task2.getInterval());
  }

  @Test
  public void testRestoreTaskSerde() throws Exception
  {
    final RestoreTask task = new RestoreTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final RestoreTask task2 = (RestoreTask) jsonMapper.readValue(json, Task.class);

    JUnit5Assertions.assertEquals(task.getDataSource(), "foo");
    JUnit5Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    JUnit5Assertions.assertEquals(task.getId(), task2.getId());
    JUnit5Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    JUnit5Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    JUnit5Assertions.assertEquals(task.getInterval(), task2.getInterval());
  }

  @Test
  public void testMoveTaskSerde() throws Exception
  {
    final MoveTask task = new MoveTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        ImmutableMap.of("bucket", "hey", "baseKey", "what"),
        null,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final MoveTask task2 = (MoveTask) jsonMapper.readValue(json, Task.class);

    JUnit5Assertions.assertEquals(task.getDataSource(), "foo");
    JUnit5Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());
    JUnit5Assertions.assertEquals(ImmutableMap.<String, Object>of("bucket", "hey", "baseKey", "what"), task.getTargetLoadSpec());

    JUnit5Assertions.assertEquals(task.getId(), task2.getId());
    JUnit5Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    JUnit5Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    JUnit5Assertions.assertEquals(task.getInterval(), task2.getInterval());
    JUnit5Assertions.assertEquals(task.getTargetLoadSpec(), task2.getTargetLoadSpec());
  }
}
