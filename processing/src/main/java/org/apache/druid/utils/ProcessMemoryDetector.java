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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Detects the hard memory limit visible to the current process.
 *
 * <p>Linux containers expose a cgroup limit, but the cgroup filesystem is mounted at different locations and may be
 * either version 1 or version 2. The detector probes both versions, then considers the process address-space limit
 * and the JVM operating-system view. The smallest usable value is returned because it is the only bound that is safe
 * across all scopes.</p>
 */
@SuppressFBWarnings(
    value = "DMI_HARDCODED_ABSOLUTE_FILENAME",
    justification = "These fixed Linux procfs and cgroup paths are required for container memory detection."
)
public class ProcessMemoryDetector
{
  private static final Path DEFAULT_CGROUP_ROOT = Paths.get("/sys/fs/cgroup");
  private static final Path DEFAULT_SELF_CGROUP = Paths.get("/proc/self/cgroup");
  private static final Path DEFAULT_SELF_LIMITS = Paths.get("/proc/self/limits");

  /** Values at or above this are the Linux "unlimited" sentinel in practice. */
  private static final long MAX_REALISTIC_MEMORY_LIMIT = 1L << 60;

  private final Path cgroupRoot;
  private final Path selfCgroupFile;
  private final Path selfLimitsFile;
  private final LongSupplier operatingSystemMemorySupplier;

  public ProcessMemoryDetector()
  {
    this(DEFAULT_CGROUP_ROOT, DEFAULT_SELF_CGROUP, DEFAULT_SELF_LIMITS, JvmUtils::getTotalMemory);
  }

  @VisibleForTesting
  ProcessMemoryDetector(
      final Path cgroupRoot,
      final Path selfCgroupFile,
      final Path selfLimitsFile,
      final LongSupplier operatingSystemMemorySupplier
  )
  {
    this.cgroupRoot = cgroupRoot;
    this.selfCgroupFile = selfCgroupFile;
    this.selfLimitsFile = selfLimitsFile;
    this.operatingSystemMemorySupplier = operatingSystemMemorySupplier;
  }

  public ProcessMemoryLimit detect()
  {
    final List<DetectedLimit> limits = new ArrayList<>();
    readCgroupLimit().ifPresent(limits::add);
    readProcessLimit().ifPresent(limits::add);

    try {
      final long operatingSystemLimit = operatingSystemMemorySupplier.getAsLong();
      if (isUsableLimit(operatingSystemLimit)) {
        limits.add(new DetectedLimit(operatingSystemLimit, ProcessMemoryLimit.Source.OPERATING_SYSTEM));
      }
    }
    catch (RuntimeException ignored) {
      // The operating-system MXBean is optional on some JVM implementations.
    }

    if (limits.isEmpty()) {
      return new ProcessMemoryLimit(ProcessMemoryLimit.UNKNOWN_BYTES, ProcessMemoryLimit.Source.UNKNOWN);
    }

    DetectedLimit smallest = limits.get(0);
    for (int i = 1; i < limits.size(); i++) {
      final DetectedLimit candidate = limits.get(i);
      if (candidate.bytes < smallest.bytes) {
        smallest = candidate;
      }
    }
    return new ProcessMemoryLimit(smallest.bytes, smallest.source);
  }

  private Optional<DetectedLimit> readCgroupLimit()
  {
    final Optional<String> cgroupPath = readFirstMatchingCgroupPath();
    final Optional<Long> cgroupV2Limit = readFirstUsableLimit(
        cgroupCandidates("memory.max", cgroupPath, false)
    );
    if (cgroupV2Limit.isPresent()) {
      return cgroupV2Limit.map(bytes -> new DetectedLimit(bytes, ProcessMemoryLimit.Source.CGROUP_V2));
    }

    return readFirstUsableLimit(cgroupCandidates("memory.limit_in_bytes", cgroupPath, true))
        .map(bytes -> new DetectedLimit(bytes, ProcessMemoryLimit.Source.CGROUP_V1));
  }

  private Optional<DetectedLimit> readProcessLimit()
  {
    try {
      for (String line : Files.readAllLines(selfLimitsFile)) {
        if (!line.trim().startsWith("Max address space")) {
          continue;
        }

        final String[] fields = line.trim().split("\\s+");
        if (fields.length < 5) {
          return Optional.empty();
        }

        // The soft limit is enforced now. If it is unlimited but the hard limit is finite, the hard limit is the
        // only usable bound available from /proc.
        final Optional<Long> softLimit = parseLimit(fields[3]);
        final Optional<Long> hardLimit = parseLimit(fields[4]);
        final Optional<Long> processLimit = softLimit.isPresent() ? softLimit : hardLimit;
        return processLimit.map(bytes -> new DetectedLimit(bytes, ProcessMemoryLimit.Source.PROCESS_RLIMIT));
      }
    }
    catch (IOException | SecurityException ignored) {
      // The /proc filesystem is optional and may be inaccessible in a restricted runtime.
    }
    return Optional.empty();
  }

  private Optional<String> readFirstMatchingCgroupPath()
  {
    try {
      for (String line : Files.readAllLines(selfCgroupFile)) {
        final String[] fields = line.split(":", 3);
        if (fields.length == 3 && (fields[1].isEmpty() || containsMemoryController(fields[1]))) {
          return Optional.of(fields[2]);
        }
      }
    }
    catch (IOException | SecurityException ignored) {
      // The /proc filesystem is optional and may be inaccessible in a restricted runtime.
    }
    return Optional.empty();
  }

  private static boolean containsMemoryController(final String controllers)
  {
    for (String controller : controllers.split(",")) {
      if ("memory".equals(controller)) {
        return true;
      }
    }
    return false;
  }

  private List<Path> cgroupCandidates(
      final String fileName,
      final Optional<String> cgroupPath,
      final boolean versionOne
  )
  {
    final List<Path> candidates = new ArrayList<>();
    if (cgroupPath.isPresent()) {
      final String cgroupPathValue = cgroupPath.get();
      int firstNonSlash = 0;
      while (firstNonSlash < cgroupPathValue.length() && cgroupPathValue.charAt(firstNonSlash) == '/') {
        firstNonSlash++;
      }
      final String relativePath = cgroupPathValue.substring(firstNonSlash);
      if (!relativePath.isEmpty()) {
        candidates.add(cgroupRoot.resolve(relativePath).resolve(fileName));
        if (versionOne) {
          candidates.add(cgroupRoot.resolve("memory").resolve(relativePath).resolve(fileName));
        }
      }
    }

    candidates.add(cgroupRoot.resolve(fileName));
    if (versionOne) {
      candidates.add(cgroupRoot.resolve("memory").resolve(fileName));
    }
    return candidates;
  }

  private static Optional<Long> readFirstUsableLimit(final List<Path> candidates)
  {
    for (Path candidate : candidates) {
      try {
        if (Files.isRegularFile(candidate)) {
          final Optional<Long> limit = parseLimit(Files.readString(candidate).trim());
          if (limit.isPresent()) {
            return limit;
          }
        }
      }
      catch (IOException | SecurityException ignored) {
        // Try the next cgroup layout or source.
      }
    }
    return Optional.empty();
  }

  private static Optional<Long> parseLimit(final String value)
  {
    if (value.isEmpty() || "max".equalsIgnoreCase(value) || "unlimited".equalsIgnoreCase(value)) {
      return Optional.empty();
    }

    try {
      final long parsed = Long.parseLong(value);
      return isUsableLimit(parsed) ? Optional.of(parsed) : Optional.empty();
    }
    catch (NumberFormatException ignored) {
      return Optional.empty();
    }
  }

  private static boolean isUsableLimit(final long value)
  {
    return value > 0 && value < MAX_REALISTIC_MEMORY_LIMIT;
  }

  private record DetectedLimit(long bytes, ProcessMemoryLimit.Source source)
  {
  }
}
