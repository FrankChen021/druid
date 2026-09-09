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
  public enum Selection
  {
    ALL,
    TIME_CHUNK_LOCK,
    SEGMENT_LOCK,
    NON_SEGMENT_LOCK_WITH_NULL_GRANULARITY,
    NON_NULL_GRANULARITY_NOT_FINER_THAN_SIX_HOUR,
    SIX_HOUR_GRANULARITY,
    SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL,
    NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY,
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
        case NON_NULL_GRANULARITY_NOT_FINER_THAN_SIX_HOUR:
          return configuration.getSegmentGranularity() != null
                 && !configuration.getSegmentGranularity().isFinerThan(Granularities.SIX_HOUR);
        case SIX_HOUR_GRANULARITY:
          return Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity());
        case SIX_HOUR_GRANULARITY_AND_TEST_INTERVAL:
          return Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity())
                 && CompactionTaskRunBase.TEST_INTERVAL.equals(configuration.getInputInterval());
        case NON_SEGMENT_LOCK_WITH_SIX_HOUR_GRANULARITY:
          return configuration.getLockGranularity() != LockGranularity.SEGMENT
                 && Granularities.SIX_HOUR.equals(configuration.getSegmentGranularity());
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
  @ArgumentsSource(SelectionArgumentsProvider.class)
  public @interface CompactionTest
  {
    Selection value();
  }

  public static class SelectionArgumentsProvider implements ArgumentsProvider
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
                                  .filter(compactionTest.value()::isApplicable)
                                  .map(Arguments::of);
    }
  }

  private CompactionTaskRunTestCases()
  {
  }
}
