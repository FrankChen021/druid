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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;

public class CompactionSupervisorSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private CompactionScheduler scheduler;

  @BeforeEach
  public void setUp()
  {
    scheduler = Mockito.mock(CompactionScheduler.class);
    Mockito.when(scheduler.validateCompactionConfig(ArgumentMatchers.any()))
           .thenReturn(CompactionConfigValidationResult.success());

    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(CompactionScheduler.class, scheduler)
    );
    OBJECT_MAPPER.registerModules(
        new SupervisorModule().getJacksonModules()
    );
  }

  @Test
  public void testSerdeOfActiveSpec()
  {
    testSerde(
        new CompactionSupervisorSpec(
            InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
            false,
            scheduler
        )
    );
  }

  @Test
  public void testSerdeOfSuspendedSpec()
  {
    testSerde(
        new CompactionSupervisorSpec(
            InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
            true,
            scheduler
        )
    );
  }

  @Test
  public void testGetStatusWithInvalidSpec()
  {
    Mockito.when(scheduler.validateCompactionConfig(ArgumentMatchers.any()))
           .thenReturn(CompactionConfigValidationResult.failure("bad spec"));
    JUnit5Assertions.assertEquals(new CompactionSupervisorSpec(
            new InlineSchemaDataSourceCompactionConfig.Builder().forDataSource("datasource").build(),
            false,
            scheduler
        ).createSupervisor().getStatus().getPayload().getMessage(), "Compaction supervisor spec is invalid. Reason[bad spec].");
  }

  @Test
  public void testValidateSpecThrowsForInvalidConfig()
  {
    Mockito.when(scheduler.validateCompactionConfig(ArgumentMatchers.any()))
           .thenReturn(CompactionConfigValidationResult.failure("bad spec"));
    final CompactionSupervisorSpec invalidSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        false,
        scheduler
    );
    final DruidException thrown = JUnit5Assertions.assertThrows(
        DruidException.class,
        invalidSpec::validateSpec
    );
    JUnit5Assertions.assertEquals(DruidException.Category.INVALID_INPUT, thrown.getCategory());
    JUnit5Assertions.assertEquals(thrown.getMessage(), "Invalid compaction supervisor spec: bad spec");
  }

  @Test
  public void testValidateSpecSucceedsForValidConfig()
  {
    final CompactionSupervisorSpec validSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        false,
        scheduler
    );
    validSpec.validateSpec();
  }

  @Test
  public void testGetIdAndDataSources()
  {
    final CompactionSupervisorSpec activeSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        false,
        scheduler
    );
    JUnit5Assertions.assertEquals(activeSpec.getId(), "autocompact__wiki");
    JUnit5Assertions.assertEquals(Collections.singletonList(TestDataSource.WIKI), activeSpec.getDataSources());
    JUnit5Assertions.assertFalse(activeSpec.isSuspended());
  }

  @Test
  public void testStartStopSupervisorForActiveSpec()
  {
    Mockito.when(scheduler.isRunning()).thenReturn(true);

    final DataSourceCompactionConfig spec
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(TestDataSource.WIKI)
                                                .build();
    final CompactionSupervisorSpec activeSpec
        = new CompactionSupervisorSpec(spec, false, scheduler);

    final CompactionSupervisor supervisor = activeSpec.createSupervisor();
    JUnit5Assertions.assertEquals(CompactionSupervisor.State.RUNNING, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(1)).startCompaction(TestDataSource.WIKI, supervisor);
    Mockito.verify(scheduler, Mockito.times(1)).stopCompaction(TestDataSource.WIKI);
  }

  @Test
  public void testStartStopSupervisorWhenSchedulerStopped()
  {
    final DataSourceCompactionConfig spec
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(TestDataSource.WIKI)
                                                .build();
    final CompactionSupervisorSpec activeSpec
        = new CompactionSupervisorSpec(spec, false, scheduler);

    final CompactionSupervisor supervisor = activeSpec.createSupervisor();
    JUnit5Assertions.assertEquals(CompactionSupervisor.State.SCHEDULER_STOPPED, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(1)).startCompaction(TestDataSource.WIKI, supervisor);
    Mockito.verify(scheduler, Mockito.times(1)).stopCompaction(TestDataSource.WIKI);
  }

  @Test
  public void testStartStopSupervisorForSuspendedSpec()
  {
    Mockito.when(scheduler.isRunning()).thenReturn(true);

    final DataSourceCompactionConfig spec
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(TestDataSource.WIKI)
                                                .build();
    final CompactionSupervisorSpec suspendedSpec
        = new CompactionSupervisorSpec(spec, true, scheduler);

    final CompactionSupervisor supervisor = suspendedSpec.createSupervisor();
    JUnit5Assertions.assertEquals(CompactionSupervisor.State.SUSPENDED, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(2)).stopCompaction(TestDataSource.WIKI);
  }

  @Test
  public void testCreateSuspendedSpec()
  {
    final CompactionSupervisorSpec activeSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        false,
        scheduler
    );
    JUnit5Assertions.assertFalse(activeSpec.isSuspended());

    final CompactionSupervisorSpec suspendedSpec = activeSpec.createSuspendedSpec();
    JUnit5Assertions.assertTrue(suspendedSpec.isSuspended());
    JUnit5Assertions.assertEquals(activeSpec.getId(), suspendedSpec.getId());
    JUnit5Assertions.assertEquals(activeSpec.getSpec(), suspendedSpec.getSpec());
    JUnit5Assertions.assertEquals(activeSpec.getDataSources(), suspendedSpec.getDataSources());
  }

  @Test
  public void testCreateRunningSpec()
  {
    final CompactionSupervisorSpec suspendedSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        true,
        scheduler
    );
    JUnit5Assertions.assertTrue(suspendedSpec.isSuspended());

    final CompactionSupervisorSpec activeSpec = suspendedSpec.createRunningSpec();
    JUnit5Assertions.assertFalse(activeSpec.isSuspended());
    JUnit5Assertions.assertEquals(activeSpec.getId(), suspendedSpec.getId());
    JUnit5Assertions.assertEquals(activeSpec.getSpec(), suspendedSpec.getSpec());
    JUnit5Assertions.assertEquals(activeSpec.getDataSources(), suspendedSpec.getDataSources());
  }

  @Test
  public void test_getInputSourceResources_returnsEmpty()
  {
    final CompactionSupervisorSpec supervisorSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        true,
        scheduler
    );
    JUnit5Assertions.assertTrue(supervisorSpec.getInputSourceResources().isEmpty());
  }

  private void testSerde(CompactionSupervisorSpec spec)
  {
    try {
      String json = OBJECT_MAPPER.writeValueAsString(spec);
      SupervisorSpec deserialized = OBJECT_MAPPER.readValue(json, SupervisorSpec.class);
      JUnit5Assertions.assertTrue(deserialized instanceof CompactionSupervisorSpec);

      final CompactionSupervisorSpec observedSpec = (CompactionSupervisorSpec) deserialized;
      JUnit5Assertions.assertEquals(spec.isSuspended(), observedSpec.isSuspended());
      JUnit5Assertions.assertEquals(spec.getSpec(), observedSpec.getSpec());
      JUnit5Assertions.assertEquals(spec.getId(), observedSpec.getId());
      JUnit5Assertions.assertEquals(spec.getDataSources(), observedSpec.getDataSources());
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error while performing serde");
    }
  }
}
