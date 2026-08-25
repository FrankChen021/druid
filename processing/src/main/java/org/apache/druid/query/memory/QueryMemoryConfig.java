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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.ProcessMemoryLimit;
import org.apache.druid.utils.RuntimeInfo;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Static configuration and startup-time sizing for query execution memory.
 *
 * <p>The defaults are intentionally conservative. The manager owns only the resolved node budget; Java heap,
 * memory-mapped segments, thread stacks, and other process allocations remain outside that budget.</p>
 */
public class QueryMemoryConfig
{
  public static final String AUTOMATIC = "auto";
  public static final HumanReadableBytes AUTOMATIC_BYTES = HumanReadableBytes.valueOf(-1);
  public static final long DEFAULT_MINIMUM_PER_QUERY_BYTES = HumanReadableBytes.parse("32MiB");
  public static final Period DEFAULT_ALLOCATION_TIMEOUT = Period.seconds(30);

  private static final Logger log = new Logger(QueryMemoryConfig.class);
  private static final long MIN_SYSTEM_RESERVE_BYTES = HumanReadableBytes.parse("256MiB");
  private static final long MAX_SYSTEM_RESERVE_BYTES = HumanReadableBytes.parse("4GiB");
  private static final long SYSTEM_RESERVE_FRACTION_DENOMINATOR = 10;

  @JsonProperty
  private final Mode mode;
  @JsonProperty
  private final HumanReadableBytes maxBytes;
  @JsonProperty
  private final HumanReadableBytes processLimit;
  @JsonProperty
  private final HumanReadableBytes systemReserve;
  @JsonProperty
  private final HumanReadableBytes maxPerQuery;
  @JsonProperty
  private final HumanReadableBytes minimumPerQuery;
  @JsonProperty
  private final Period allocationTimeout;

  private final ProcessMemoryLimit detectedProcessLimit;
  private final long resolvedProcessLimitBytes;
  private final long resolvedSystemReserveBytes;
  private final long resolvedMaxBytes;
  private final long resolvedMaxPerQueryBytes;

  @JsonCreator
  public QueryMemoryConfig(
      @JsonProperty("mode") @Nullable String mode,
      @JsonProperty("maxBytes") @JsonDeserialize(using = AutoHumanReadableBytesDeserializer.class)
      @Nullable HumanReadableBytes maxBytes,
      @JsonProperty("processLimit") @JsonDeserialize(using = AutoHumanReadableBytesDeserializer.class)
      @Nullable HumanReadableBytes processLimit,
      @JsonProperty("systemReserve") @JsonDeserialize(using = AutoHumanReadableBytesDeserializer.class)
      @Nullable HumanReadableBytes systemReserve,
      @JsonProperty("maxPerQuery") @JsonDeserialize(using = AutoHumanReadableBytesDeserializer.class)
      @Nullable HumanReadableBytes maxPerQuery,
      @JsonProperty("minimumPerQuery") @Nullable HumanReadableBytes minimumPerQuery,
      @JsonProperty("allocationTimeout") @Nullable Period allocationTimeout,
      @JacksonInject final RuntimeInfo runtimeInfo
  )
  {
    this.mode = parseMode(mode);
    this.maxBytes = Configs.valueOrDefault(maxBytes, AUTOMATIC_BYTES);
    this.processLimit = Configs.valueOrDefault(processLimit, AUTOMATIC_BYTES);
    this.systemReserve = Configs.valueOrDefault(systemReserve, AUTOMATIC_BYTES);
    this.maxPerQuery = Configs.valueOrDefault(maxPerQuery, AUTOMATIC_BYTES);
    this.minimumPerQuery = Configs.valueOrDefault(
        minimumPerQuery,
        HumanReadableBytes.valueOf(DEFAULT_MINIMUM_PER_QUERY_BYTES)
    );
    this.allocationTimeout = Configs.valueOrDefault(allocationTimeout, DEFAULT_ALLOCATION_TIMEOUT);

    final long minimumPerQueryBytes = this.minimumPerQuery.getBytes();
    if (minimumPerQueryBytes <= 0) {
      throw new IAE("druid.processing.memory.minimumPerQuery must be greater than zero");
    }
    if (this.allocationTimeout.toStandardDuration().getMillis() < 0) {
      throw new IAE("druid.processing.memory.allocationTimeout must not be negative");
    }

    this.detectedProcessLimit = runtimeInfo.getProcessMemoryLimit();
    this.resolvedProcessLimitBytes = resolveProcessLimit(this.processLimit, this.detectedProcessLimit, runtimeInfo);
    this.resolvedSystemReserveBytes = resolveSystemReserve(this.systemReserve, this.resolvedProcessLimitBytes);

    final long availableBytes = subtractSafely(
        subtractSafely(this.resolvedProcessLimitBytes, runtimeInfo.getMaxHeapSizeBytes()),
        this.resolvedSystemReserveBytes
    );
    final long configuredMaxBytes = parseSetting(this.maxBytes, "druid.processing.memory.maxBytes");
    this.resolvedMaxBytes = configuredMaxBytes == -1
                            ? availableBytes
                            : Math.min(configuredMaxBytes, availableBytes);

    final long configuredMaxPerQueryBytes = parseSetting(
        this.maxPerQuery,
        "druid.processing.memory.maxPerQuery"
    );
    this.resolvedMaxPerQueryBytes = configuredMaxPerQueryBytes == -1
                                    ? this.resolvedMaxBytes
                                    : Math.min(configuredMaxPerQueryBytes, this.resolvedMaxBytes);

    log.info(
        "Query memory plan: mode[%s], detectedProcessLimit[%,d] bytes (%s), processLimit[%,d] bytes, "
        + "maxHeap[%,d] bytes, systemReserve[%,d] bytes, nodeBudget[%,d] bytes, maxPerQuery[%,d] bytes, "
        + "minimumPerQuery[%,d] bytes",
        this.mode,
        detectedProcessLimit.bytes(),
        detectedProcessLimit.source(),
        this.resolvedProcessLimitBytes,
        runtimeInfo.getMaxHeapSizeBytes(),
        this.resolvedSystemReserveBytes,
        this.resolvedMaxBytes,
        this.resolvedMaxPerQueryBytes,
        minimumPerQueryBytes
    );
  }

  public QueryMemoryConfig()
  {
    this(null, null, null, null, null, null, null, new RuntimeInfo());
  }

  public Mode getMode()
  {
    return mode;
  }

  /** Returns the detected process/container limit, before any explicit override. */
  public ProcessMemoryLimit getDetectedProcessLimit()
  {
    return detectedProcessLimit;
  }

  /** Returns the process limit used for sizing after applying {@code processLimit}. */
  public long getProcessLimitBytes()
  {
    return resolvedProcessLimitBytes;
  }

  public long getSystemReserveBytes()
  {
    return resolvedSystemReserveBytes;
  }

  /** Returns the hard node budget available to query execution memory. */
  public long getMaxBytes()
  {
    return resolvedMaxBytes;
  }

  public long getMaxPerQueryBytes()
  {
    return resolvedMaxPerQueryBytes;
  }

  public long getMinimumPerQueryBytes()
  {
    return minimumPerQuery.getBytes();
  }

  public Period getAllocationTimeout()
  {
    return allocationTimeout;
  }

  public enum Mode
  {
    LEGACY,
    ACCOUNTED,
    FFM
  }

  private static Mode parseMode(final String value)
  {
    if (value == null || value.isBlank()) {
      return Mode.LEGACY;
    }
    try {
      return Mode.valueOf(StringUtils.toUpperCase(value.trim()));
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Invalid value[%s] for druid.processing.memory.mode. Expected legacy, accounted, or ffm", value);
    }
  }

  private static long resolveProcessLimit(
      final HumanReadableBytes configuredValue,
      final ProcessMemoryLimit detectedLimit,
      final RuntimeInfo runtimeInfo
  )
  {
    final long configuredBytes = parseSetting(configuredValue, "druid.processing.memory.processLimit");
    if (configuredBytes != -1) {
      return configuredBytes;
    }
    if (detectedLimit.isKnown()) {
      return detectedLimit.bytes();
    }

    // If neither cgroups nor the OS bean is available, retain the existing JVM heap/direct-memory safety boundary.
    final long maxHeapBytes = runtimeInfo.getMaxHeapSizeBytes();
    try {
      return Math.addExact(maxHeapBytes, runtimeInfo.getDirectMemorySizeBytes());
    }
    catch (ArithmeticException | UnsupportedOperationException e) {
      return maxHeapBytes;
    }
  }

  private static long resolveSystemReserve(
      final HumanReadableBytes configuredValue,
      final long processLimitBytes
  )
  {
    final long configuredBytes = parseSetting(configuredValue, "druid.processing.memory.systemReserve");
    if (configuredBytes != -1) {
      return configuredBytes;
    }

    final long fraction = processLimitBytes / SYSTEM_RESERVE_FRACTION_DENOMINATOR;
    final long minimum = Math.min(MIN_SYSTEM_RESERVE_BYTES, processLimitBytes / 4);
    return Math.min(MAX_SYSTEM_RESERVE_BYTES, Math.max(fraction, minimum));
  }

  private static long parseSetting(final HumanReadableBytes value, final String propertyName)
  {
    if (value.equals(AUTOMATIC_BYTES)) {
      return -1;
    }
    final long bytes = value.getBytes();
    if (bytes < 0) {
      throw new IAE("%s must not be negative", propertyName);
    }
    return bytes;
  }

  private static long subtractSafely(final long left, final long right)
  {
    if (left <= 0) {
      return 0;
    }
    return right >= left ? 0 : left - right;
  }

  /**
   * Parses the normal {@link HumanReadableBytes} syntax and additionally accepts {@code auto}, which maps to the
   * same negative sentinel used by the existing processing buffer configuration.
   */
  private static class AutoHumanReadableBytesDeserializer extends JsonDeserializer<HumanReadableBytes>
  {
    @Override
    public HumanReadableBytes deserialize(
        final JsonParser parser,
        final DeserializationContext context
    ) throws IOException
    {
      if (parser.currentToken() == JsonToken.VALUE_STRING) {
        final String value = parser.getText().trim();
        return AUTOMATIC.equalsIgnoreCase(value) ? AUTOMATIC_BYTES : new HumanReadableBytes(value);
      }
      if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
        return HumanReadableBytes.valueOf(parser.getLongValue());
      }
      if (parser.currentToken() == JsonToken.VALUE_NULL) {
        return null;
      }
      return (HumanReadableBytes) context.handleUnexpectedToken(HumanReadableBytes.class, parser);
    }
  }
}
