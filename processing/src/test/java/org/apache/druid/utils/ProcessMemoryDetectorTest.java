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

package org.apache.druid.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ProcessMemoryDetectorTest
{
  private static final long GIB = 1024L * 1024 * 1024;

  @TempDir
  public Path temporaryDirectory;

  @Test
  public void testCgroupV2LimitWinsOverOperatingSystemView() throws IOException
  {
    final Path cgroupRoot = temporaryDirectory.resolve("sys/fs/cgroup");
    final Path cgroupDirectory = cgroupRoot.resolve("docker/container");
    final Path selfCgroup = temporaryDirectory.resolve("proc/self/cgroup");
    final Path selfLimits = temporaryDirectory.resolve("proc/self/limits");
    Files.createDirectories(cgroupDirectory);
    Files.createDirectories(selfCgroup.getParent());
    Files.writeString(selfCgroup, "0::/docker/container\n");
    Files.writeString(selfLimits, "Max address space         unlimited            unlimited            bytes\n");
    Files.writeString(cgroupDirectory.resolve("memory.max"), String.valueOf(2 * GIB));

    final ProcessMemoryLimit limit = new ProcessMemoryDetector(
        cgroupRoot,
        selfCgroup,
        selfLimits,
        () -> 8 * GIB
    ).detect();

    Assertions.assertEquals(2 * GIB, limit.bytes());
    Assertions.assertEquals(ProcessMemoryLimit.Source.CGROUP_V2, limit.source());
  }

  @Test
  public void testCgroupV1NestedLimitIsDetected() throws IOException
  {
    final Path cgroupRoot = temporaryDirectory.resolve("sys/fs/cgroup");
    final Path cgroupDirectory = cgroupRoot.resolve("memory/docker/container");
    final Path selfCgroup = temporaryDirectory.resolve("proc/self/cgroup");
    final Path selfLimits = temporaryDirectory.resolve("proc/self/limits");
    Files.createDirectories(cgroupDirectory);
    Files.createDirectories(selfCgroup.getParent());
    Files.writeString(selfCgroup, "5:memory:/docker/container\n");
    Files.writeString(selfLimits, "Max address space         unlimited            unlimited            bytes\n");
    Files.writeString(cgroupDirectory.resolve("memory.limit_in_bytes"), String.valueOf(GIB));

    final ProcessMemoryLimit limit = new ProcessMemoryDetector(
        cgroupRoot,
        selfCgroup,
        selfLimits,
        () -> 8 * GIB
    ).detect();

    Assertions.assertEquals(GIB, limit.bytes());
    Assertions.assertEquals(ProcessMemoryLimit.Source.CGROUP_V1, limit.source());
  }

  @Test
  public void testProcessLimitIsUsedWhenCgroupIsUnlimited() throws IOException
  {
    final Path cgroupRoot = temporaryDirectory.resolve("sys/fs/cgroup");
    final Path selfCgroup = temporaryDirectory.resolve("proc/self/cgroup");
    final Path selfLimits = temporaryDirectory.resolve("proc/self/limits");
    Files.createDirectories(cgroupRoot);
    Files.createDirectories(selfCgroup.getParent());
    Files.writeString(selfCgroup, "0::/\n");
    Files.writeString(cgroupRoot.resolve("memory.max"), "max\n");
    Files.createDirectories(selfLimits.getParent());
    Files.writeString(
        selfLimits,
        "Max address space         " + (2 * GIB) + "                  " + (4 * GIB) + "                  bytes\n"
    );

    final ProcessMemoryLimit limit = new ProcessMemoryDetector(
        cgroupRoot,
        selfCgroup,
        selfLimits,
        () -> 8 * GIB
    ).detect();

    Assertions.assertEquals(2 * GIB, limit.bytes());
    Assertions.assertEquals(ProcessMemoryLimit.Source.PROCESS_RLIMIT, limit.source());
  }

  @Test
  public void testUnknownWhenNoSourceIsAvailable() throws IOException
  {
    final ProcessMemoryLimit limit = new ProcessMemoryDetector(
        temporaryDirectory.resolve("missing-cgroup"),
        temporaryDirectory.resolve("missing-cgroup"),
        temporaryDirectory.resolve("missing-limits"),
        () -> ProcessMemoryLimit.UNKNOWN_BYTES
    ).detect();

    Assertions.assertEquals(ProcessMemoryLimit.UNKNOWN_BYTES, limit.bytes());
    Assertions.assertEquals(ProcessMemoryLimit.Source.UNKNOWN, limit.source());
  }
}
