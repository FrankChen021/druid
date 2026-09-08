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

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.annotation.Nullable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.stream.Stream;

public final class CompactionTaskRunTestCases
{
  public enum Scenario
  {
    RUN_WITH_DYNAMIC_PARTITIONING(Selection.ALL),
    RUN_WITH_HASH_PARTITIONING(Selection.NON_SEGMENT_LOCK_WITH_NULL_GRANULARITY),
    RUN_COMPACTION_TWICE(Selection.TIME_CHUNK_LOCK),
    RUN_COMPACTION_TWICE_WITH_SEGMENT_LOCK(Selection.SEGMENT_LOCK),
    RUN_INDEX_AND_COMPACT_AT_THE_SAME_TIME_FOR_DIFFERENT_INTERVAL(
        Selection.NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    WITH_SEGMENT_GRANULARITY_MISALIGNED_INTERVAL(Selection.SIX_HOUR_GRANULARITY),
    WITH_SEGMENT_GRANULARITY_MISALIGNED_INTERVAL_ALLOWED(Selection.SIX_HOUR_GRANULARITY),
    WITH_SEGMENT_GRANULARITY_MISALIGNED_INTERVAL_ALLOWED_2(
        Selection.NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    COMPACTION_WITH_FILTER_IN_TRANSFORM_SPEC(Selection.SIX_HOUR_GRANULARITY),
    COMPACTION_WITH_NEW_METRIC_IN_METRICS_SPEC(Selection.SIX_HOUR_GRANULARITY),
    WITH_GRANULARITY_SPEC_NON_NULL_QUERY_GRANULARITY(Selection.ALL),
    WITH_GRANULARITY_SPEC_NON_NULL_QUERY_GRANULARITY_AND_COARSE_SEGMENT_GRANULARITY(
        Selection.SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    COMPACT_THEN_APPEND(Selection.SIX_HOUR_GRANULARITY),
    PARTIAL_INTERVAL_COMPACT_WITH_FINER_SEGMENT_GRANULARITY_THAN_FULL_INTERVAL_COMPACT_WITH_DROP_EXISTING_TRUE(
        Selection.NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    COMPACT_DATASOURCE_OVER_INTERVAL_WITH_ONLY_TOMBSTONES(
        Selection.NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    PARTIAL_INTERVAL_COMPACT_WITH_FINER_SEGMENT_GRANULARITY_THEN_FULL_INTERVAL_COMPACT_WITH_DROP_EXISTING_FALSE(
        Selection.NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL
    ),
    RUN_INDEX_AND_COMPACT_FOR_SAME_SEGMENT_AT_THE_SAME_TIME(Selection.ALL),
    RUN_INDEX_AND_COMPACT_FOR_SAME_SEGMENT_AT_THE_SAME_TIME_2(Selection.ALL),
    RUN_WITH_SPATIAL_DIMENSIONS(Selection.SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL),
    RUN_WITH_AUTO_CAST_DIMENSIONS(Selection.SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL),
    RUN_WITH_AUTO_CAST_DIMENSIONS_SORT_BY_DIMENSION(Selection.SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL);

    private final Selection selection;

    Scenario(Selection selection)
    {
      this.selection = selection;
    }

    public boolean isApplicable(Configuration configuration)
    {
      return selection.isApplicable(configuration);
    }
  }

  private enum Selection
  {
    ALL,
    TIME_CHUNK_LOCK,
    SEGMENT_LOCK,
    NON_SEGMENT_LOCK_WITH_NULL_GRANULARITY,
    SIX_HOUR_GRANULARITY,
    SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL,
    NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL;

    boolean isApplicable(Configuration configuration)
    {
      switch (this) {
        case ALL:
          return true;
        case TIME_CHUNK_LOCK:
          return configuration.getLockGranularity() == LockGranularity.TIME_CHUNK;
        case SEGMENT_LOCK:
          return configuration.getLockGranularity() == LockGranularity.SEGMENT;
        case NON_SEGMENT_LOCK_WITH_NULL_GRANULARITY:
          return configuration.getLockGranularity() != LockGranularity.SEGMENT
                 && configuration.getSegmentGranularity() == null;
        case SIX_HOUR_GRANULARITY:
          return Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity());
        case SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL:
          return Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity())
                 && CompactionTaskRunBase.TEST_INTERVAL.equals(configuration.getInputInterval());
        case NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL:
          return configuration.getLockGranularity() != LockGranularity.SEGMENT
                 && Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity())
                 && CompactionTaskRunBase.TEST_INTERVAL.equals(configuration.getInputInterval());
        default:
          throw new IllegalStateException("Unhandled selection " + this);
      }
    }
  }

  public static class Configuration
  {
    private final LockGranularity lockGranularity;
    private final boolean useCentralizedDatasourceSchema;
    private final boolean batchSegmentAllocation;
    private final boolean useSegmentMetadataCache;
    private final boolean useConcurrentLocks;
    private final Interval inputInterval;
    @Nullable
    private final Granularity segmentGranularity;

    public Configuration(
        LockGranularity lockGranularity,
        boolean useCentralizedDatasourceSchema,
        boolean batchSegmentAllocation,
        boolean useSegmentMetadataCache,
        boolean useConcurrentLocks,
        Interval inputInterval,
        @Nullable Granularity segmentGranularity
    )
    {
      this.lockGranularity = lockGranularity;
      this.useCentralizedDatasourceSchema = useCentralizedDatasourceSchema;
      this.batchSegmentAllocation = batchSegmentAllocation;
      this.useSegmentMetadataCache = useSegmentMetadataCache;
      this.useConcurrentLocks = useConcurrentLocks;
      this.inputInterval = inputInterval;
      this.segmentGranularity = segmentGranularity;
    }

    public LockGranularity getLockGranularity()
    {
      return lockGranularity;
    }

    public boolean isUseCentralizedDatasourceSchema()
    {
      return useCentralizedDatasourceSchema;
    }

    public boolean isBatchSegmentAllocation()
    {
      return batchSegmentAllocation;
    }

    public boolean isUseSegmentMetadataCache()
    {
      return useSegmentMetadataCache;
    }

    public boolean isUseConcurrentLocks()
    {
      return useConcurrentLocks;
    }

    public Interval getInputInterval()
    {
      return inputInterval;
    }

    @Nullable
    public Granularity getSegmentGranularity()
    {
      return segmentGranularity;
    }

    @Override
    public String toString()
    {
      return "lockGranularity=" + lockGranularity
             + ", useCentralizedDatasourceSchema=" + useCentralizedDatasourceSchema
             + ", batchSegmentAllocation=" + batchSegmentAllocation
             + ", useSegmentMetadataCache=" + useSegmentMetadataCache
             + ", useConcurrentLocks=" + useConcurrentLocks
             + ", inputInterval=" + inputInterval
             + ", segmentGranularity=" + segmentGranularity;
    }
  }

  public interface ConfigurationProvider
  {
    Stream<Configuration> configurations();

    default boolean isApplicable(Scenario scenario, Configuration configuration)
    {
      return scenario.isApplicable(configuration);
    }
  }

  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface ConfigurationSource
  {
    Class<? extends ConfigurationProvider> value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(ScenarioArgumentsProvider.class)
  public @interface CompactionTest
  {
    Scenario value();
  }

  public static class ScenarioArgumentsProvider implements ArgumentsProvider
  {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception
    {
      final CompactionTest compactionTest = context.getRequiredTestMethod().getAnnotation(CompactionTest.class);
      if (compactionTest == null) {
        throw new IllegalStateException("Missing @CompactionTest on " + context.getRequiredTestMethod());
      }

      final ConfigurationSource configurationSource = context.getRequiredTestClass()
                                                             .getAnnotation(ConfigurationSource.class);
      if (configurationSource == null) {
        throw new IllegalStateException("Missing @ConfigurationSource on " + context.getRequiredTestClass());
      }

      final ConfigurationProvider configurationProvider = configurationSource.value()
                                                                                  .getDeclaredConstructor()
                                                                                  .newInstance();
      return configurationProvider.configurations()
                                  .filter(configuration -> configurationProvider.isApplicable(
                                      compactionTest.value(),
                                      configuration
                                  ))
                                  .map(Arguments::of);
    }
  }

  private CompactionTaskRunTestCases()
  {
  }
}
