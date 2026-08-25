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

package org.apache.druid.query.memory;

import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.utils.ProcessMemoryLimit;
import org.apache.druid.utils.RuntimeInfo;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryMemoryConfigTest
{
  private static final long GIB = 1024L * 1024 * 1024;

  @Test
  public void testAutomaticBudgetUsesProcessLimitHeapAndReserve()
  {
    final QueryMemoryConfig config = new QueryMemoryConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new TestRuntimeInfo(10 * GIB, 2 * GIB, 4 * GIB)
    );

    Assertions.assertEquals(QueryMemoryConfig.Mode.LEGACY, config.getMode());
    Assertions.assertEquals(10 * GIB, config.getProcessLimitBytes());
    Assertions.assertEquals(GIB, config.getSystemReserveBytes());
    Assertions.assertEquals(7 * GIB, config.getMaxBytes());
    Assertions.assertEquals(7 * GIB, config.getMaxPerQueryBytes());
    Assertions.assertEquals(QueryMemoryConfig.DEFAULT_MINIMUM_PER_QUERY_BYTES, config.getMinimumPerQueryBytes());
    Assertions.assertEquals(30_000, config.getAllocationTimeout().toStandardDuration().getMillis());
  }

  @Test
  public void testExplicitLimitsAreAppliedAndPerQueryIsClamped()
  {
    final QueryMemoryConfig config = new QueryMemoryConfig(
        "ffm",
        new HumanReadableBytes("3GiB"),
        new HumanReadableBytes("7GiB"),
        new HumanReadableBytes("512MiB"),
        new HumanReadableBytes("10GiB"),
        new HumanReadableBytes("64MiB"),
        Period.seconds(2),
        new TestRuntimeInfo(10 * GIB, 2 * GIB, 4 * GIB)
    );

    Assertions.assertEquals(QueryMemoryConfig.Mode.FFM, config.getMode());
    Assertions.assertEquals(7 * GIB, config.getProcessLimitBytes());
    Assertions.assertEquals(512L * 1024 * 1024, config.getSystemReserveBytes());
    Assertions.assertEquals(3 * GIB, config.getMaxBytes());
    Assertions.assertEquals(3 * GIB, config.getMaxPerQueryBytes());
    Assertions.assertEquals(64L * 1024 * 1024, config.getMinimumPerQueryBytes());
    Assertions.assertEquals(2_000, config.getAllocationTimeout().toStandardDuration().getMillis());
  }

  @Test
  public void testUnknownProcessLimitFallsBackToHeapAndDirectMemory()
  {
    final QueryMemoryConfig config = new QueryMemoryConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new TestRuntimeInfo(
            new ProcessMemoryLimit(ProcessMemoryLimit.UNKNOWN_BYTES, ProcessMemoryLimit.Source.UNKNOWN),
            2 * GIB,
            4 * GIB
        )
    );

    Assertions.assertEquals(6 * GIB, config.getProcessLimitBytes());
    Assertions.assertTrue(config.getMaxBytes() > 0);
  }

  @Test
  public void testInvalidModeAndMinimumAreRejected()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryMemoryConfig(
            "unsupported",
            null,
            null,
            null,
            null,
            null,
            null,
            new TestRuntimeInfo(10 * GIB, 2 * GIB, 4 * GIB)
        )
    );

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryMemoryConfig(
            null,
            null,
            null,
            null,
            null,
            HumanReadableBytes.ZERO,
            null,
            new TestRuntimeInfo(10 * GIB, 2 * GIB, 4 * GIB)
        )
    );
  }

  private static class TestRuntimeInfo extends RuntimeInfo
  {
    private final ProcessMemoryLimit processMemoryLimit;
    private final long maxHeapSizeBytes;
    private final long directMemorySizeBytes;

    TestRuntimeInfo(final long processLimitBytes, final long maxHeapSizeBytes, final long directMemorySizeBytes)
    {
      this(
          new ProcessMemoryLimit(processLimitBytes, ProcessMemoryLimit.Source.CGROUP_V2),
          maxHeapSizeBytes,
          directMemorySizeBytes
      );
    }

    TestRuntimeInfo(
        final ProcessMemoryLimit processMemoryLimit,
        final long maxHeapSizeBytes,
        final long directMemorySizeBytes
    )
    {
      this.processMemoryLimit = processMemoryLimit;
      this.maxHeapSizeBytes = maxHeapSizeBytes;
      this.directMemorySizeBytes = directMemorySizeBytes;
    }

    @Override
    public ProcessMemoryLimit getProcessMemoryLimit()
    {
      return processMemoryLimit;
    }

    @Override
    public long getMaxHeapSizeBytes()
    {
      return maxHeapSizeBytes;
    }

    @Override
    public long getDirectMemorySizeBytes()
    {
      return directMemorySizeBytes;
    }
  }
}
