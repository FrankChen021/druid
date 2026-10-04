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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/// Owns the bounded lease and immutable worker-attempt mapping for one source query's final frames.
public class LiveFramesSession
{
  public static final int PROTOCOL_VERSION = 1;
  public static final int FRAME_FILE_VERSION = 1;
  public static final long DEFAULT_LEASE_MILLIS = TimeUnit.MINUTES.toMillis(2);
  public static final long MAX_LEASE_MILLIS = TimeUnit.MINUTES.toMillis(10);
  public static final long MAX_PARTITION_READ_MILLIS = TimeUnit.SECONDS.toMillis(30);

  private final Object lock = new Object();
  private final String queryId;
  private final String owner;
  private final LongSupplier clockMillis;
  private State state = State.SUBMITTED;
  private long leaseExpiresAtMillis;
  @Nullable
  private String failureCode;
  @Nullable
  private Manifest manifest;
  private final Map<String, PartitionLocation> partitionLocations = new LinkedHashMap<>();
  private final Map<String, Long> activePartitionReads = new HashMap<>();

  /// Creates a session using the system clock.
  public LiveFramesSession(final String queryId, final String owner)
  {
    this(queryId, owner, System::currentTimeMillis);
  }

  /// Creates a session with an injectable clock for deterministic lifecycle tests.
  public LiveFramesSession(final String queryId, final String owner, final LongSupplier clockMillis)
  {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
  }

  /// Marks the source controller as executing the query.
  public void markRunning()
  {
    synchronized (lock) {
      if (state == State.SUBMITTED) {
        state = State.RUNNING;
        lock.notifyAll();
      }
    }
  }

  /// Publishes a stable manifest once all final-stage attempts have completed.
  public void markReady(
      final StageId stageId,
      final RowSignature signature,
      final List<PartitionLocation> locations
  )
  {
    markReady(stageId, signature, identityColumnMapping(signature), locations);
  }

  /// Publishes a stable manifest with the physical frame signature and SQL output column mapping.
  public void markReady(
      final StageId stageId,
      final RowSignature signature,
      final Map<String, List<String>> columnMapping,
      final List<PartitionLocation> locations
  )
  {
    synchronized (lock) {
      if (state != State.RUNNING) {
        throw new IllegalStateException("Remote frame session is not running");
      }
      if (locations.size() > Limits.DEFAULT_MAX_PARTITIONS) {
        throw new IllegalArgumentException("Remote frame manifest exceeds the supported partition limit");
      }

      final List<Partition> partitions = new ArrayList<>(locations.size());
      for (final PartitionLocation location : locations) {
        final PartitionLocation stagedLocation = location.withFinalStageNumber(stageId.getStageNumber());
        if (partitionLocations.putIfAbsent(stagedLocation.partitionId, stagedLocation) != null) {
          throw new IllegalArgumentException("Duplicate remote frame partition identifier");
        }
        partitions.add(new Partition(stagedLocation.partitionId, stagedLocation.attemptId));
      }

      manifest = new Manifest(
          PROTOCOL_VERSION,
          FRAME_FILE_VERSION,
          stageId.getStageNumber(),
          signature,
          columnMapping,
          partitions
      );
      leaseExpiresAtMillis = Math.addExact(clockMillis.getAsLong(), DEFAULT_LEASE_MILLIS);
      state = State.READY;
      lock.notifyAll();
    }
  }

  private static Map<String, List<String>> identityColumnMapping(final RowSignature signature)
  {
    final Map<String, List<String>> mapping = new LinkedHashMap<>();
    for (final String columnName : signature.getColumnNames()) {
      mapping.put(columnName, List.of(columnName));
    }
    return mapping;
  }

  /// Returns a snapshot suitable for the versioned remote-frame API.
  public SessionInfo getInfo()
  {
    synchronized (lock) {
      updateExpiry();
      return new SessionInfo(queryId, state.apiName, manifest, failureCode, leaseExpiresAtMillis);
    }
  }

  /// Returns whether the supplied identity owns this session.
  public boolean isOwnedBy(@Nullable final String identity)
  {
    return owner.equals(identity);
  }

  /// Extends a ready lease by the requested bounded duration.
  public boolean renewLease(final long leaseMillis)
  {
    if (leaseMillis <= 0 || leaseMillis > MAX_LEASE_MILLIS) {
      throw new IllegalArgumentException("Remote frame lease must be between 1 and 600000 milliseconds");
    }

    synchronized (lock) {
      updateExpiry();
      if (state != State.READY) {
        return false;
      }
      leaseExpiresAtMillis = Math.addExact(clockMillis.getAsLong(), leaseMillis);
      lock.notifyAll();
      return true;
    }
  }

  /// Requests idempotent release and wakes the controller waiting to clean final-stage outputs.
  public void release()
  {
    synchronized (lock) {
      if (state == State.READY || state == State.EXPIRED) {
        state = State.RELEASED;
      } else if (state == State.SUBMITTED || state == State.RUNNING) {
        state = State.CANCELLED;
      }
      lock.notifyAll();
    }
  }

  /// Marks a query as failed without retaining exception messages that might disclose query values.
  public void fail()
  {
    synchronized (lock) {
      if (state != State.RELEASED && state != State.EXPIRED && state != State.CANCELLED) {
        state = State.FAILED;
        failureCode = "remoteFrameSourceFailed";
      }
      lock.notifyAll();
    }
  }

  /// Marks an interrupted source task as cancelled and releases any retained outputs.
  public void cancel()
  {
    synchronized (lock) {
      if (state != State.RELEASED && state != State.EXPIRED) {
        state = State.CANCELLED;
      }
      lock.notifyAll();
    }
  }

  /// Waits until release, expiry, cancellation, or failure so source workers stay alive only for the lease.
  public void awaitRelease() throws InterruptedException
  {
    synchronized (lock) {
      updateExpiry();
      pruneExpiredPartitionReads();
      while (state == State.READY || !activePartitionReads.isEmpty()) {
        final long now = clockMillis.getAsLong();
        long wakeAtMillis = state == State.READY ? leaseExpiresAtMillis : Long.MAX_VALUE;
        for (final long readExpiresAtMillis : activePartitionReads.values()) {
          wakeAtMillis = Math.min(wakeAtMillis, readExpiresAtMillis);
        }
        final long remainingMillis = wakeAtMillis - now;
        if (remainingMillis <= 0) {
          updateExpiry();
          pruneExpiredPartitionReads();
          continue;
        }
        TimeUnit.MILLISECONDS.timedWait(lock, remainingMillis);
        updateExpiry();
        pruneExpiredPartitionReads();
      }
    }
  }

  /// Resolves a manifest identifier to its source-local worker and stage partition.
  @Nullable
  public PartitionLocation getPartitionLocation(final String partitionId)
  {
    synchronized (lock) {
      updateExpiry();
      return state == State.READY ? partitionLocations.get(partitionId) : null;
    }
  }

  /// Starts one bounded partition response and keeps worker output alive until the response closes.
  @Nullable
  public PartitionLocation beginPartitionRead(final String partitionId)
  {
    synchronized (lock) {
      updateExpiry();
      if (state != State.READY) {
        return null;
      }
      final PartitionLocation location = partitionLocations.get(partitionId);
      if (location == null) {
        return null;
      }
      final String readId = UUID.randomUUID().toString();
      final long readExpiresAtMillis = Math.min(
          leaseExpiresAtMillis,
          Math.addExact(clockMillis.getAsLong(), MAX_PARTITION_READ_MILLIS)
      );
      activePartitionReads.put(readId, readExpiresAtMillis);
      return location.withReadLease(readId, readExpiresAtMillis);
    }
  }

  /// Closes a partition response and allows a released controller to reclaim worker output.
  public void endPartitionRead(final String readId)
  {
    synchronized (lock) {
      activePartitionReads.remove(readId);
      lock.notifyAll();
    }
  }

  private void pruneExpiredPartitionReads()
  {
    final long now = clockMillis.getAsLong();
    activePartitionReads.entrySet().removeIf(entry -> now >= entry.getValue());
  }

  private void updateExpiry()
  {
    if (state == State.READY && clockMillis.getAsLong() >= leaseExpiresAtMillis) {
      state = State.EXPIRED;
      lock.notifyAll();
    }
  }

  /// Current session state.
  public enum State
  {
    SUBMITTED("submitted"),
    RUNNING("running"),
    READY("ready"),
    RELEASED("released"),
    FAILED("failed"),
    CANCELLED("cancelled"),
    EXPIRED("expired");

    private final String apiName;

    State(final String apiName)
    {
      this.apiName = apiName;
    }
  }

  /// Source-local mapping for one immutable final-stage worker attempt.
  public static class PartitionLocation
  {
    private final String partitionId;
    private final String attemptId;
    private final String workerId;
    private final int partitionNumber;
    private final int finalStageNumber;
    @Nullable
    private final String readId;
    private final long readExpiresAtMillis;

    /// Creates an opaque manifest entry and its source-local worker mapping.
    public PartitionLocation(final String workerId, final int partitionNumber)
    {
      this(UUID.randomUUID().toString(), UUID.randomUUID().toString(), workerId, partitionNumber, -1, null, 0L);
    }

    public PartitionLocation(
        final String partitionId,
        final String attemptId,
        final String workerId,
        final int partitionNumber
    )
    {
      this(partitionId, attemptId, workerId, partitionNumber, -1, null, 0L);
    }

    /// Deserializes a partition mapping returned to the source Broker gateway.
    @JsonCreator
    public PartitionLocation(
        @JsonProperty("partitionId") final String partitionId,
        @JsonProperty("attemptId") final String attemptId,
        @JsonProperty("workerId") final String workerId,
        @JsonProperty("partitionNumber") final int partitionNumber,
        @JsonProperty("finalStageNumber") @Nullable final Integer finalStageNumber,
        @JsonProperty("readId") @Nullable final String readId,
        @JsonProperty("readExpiresAtMillis") @Nullable final Long readExpiresAtMillis
    )
    {
      this.partitionId = Objects.requireNonNull(partitionId, "partitionId");
      this.attemptId = Objects.requireNonNull(attemptId, "attemptId");
      this.workerId = Objects.requireNonNull(workerId, "workerId");
      this.partitionNumber = partitionNumber;
      this.finalStageNumber = finalStageNumber == null ? -1 : finalStageNumber;
      this.readId = readId;
      this.readExpiresAtMillis = readExpiresAtMillis == null ? 0L : readExpiresAtMillis;
    }

    private PartitionLocation withFinalStageNumber(final int stageNumber)
    {
      return new PartitionLocation(partitionId, attemptId, workerId, partitionNumber, stageNumber, null, 0L);
    }

    private PartitionLocation withReadLease(final String readId, final long readExpiresAtMillis)
    {
      return new PartitionLocation(
          partitionId,
          attemptId,
          workerId,
          partitionNumber,
          finalStageNumber,
          readId,
          readExpiresAtMillis
      );
    }

    /// Returns the opaque partition identifier included in the manifest.
    @JsonProperty
    public String getPartitionId()
    {
      return partitionId;
    }

    /// Returns the immutable worker attempt identifier included in the manifest.
    @JsonProperty
    public String getAttemptId()
    {
      return attemptId;
    }

    /// Returns the source-local worker ID.
    @JsonProperty
    public String getWorkerId()
    {
      return workerId;
    }

    /// Returns the final-stage partition number on the source worker.
    @JsonProperty
    public int getPartitionNumber()
    {
      return partitionNumber;
    }

    /// Returns the final stage number for the retained frame partition.
    @JsonProperty
    public int getFinalStageNumber()
    {
      return finalStageNumber;
    }

    /// Returns the opaque identifier used to close this bounded partition response.
    @JsonProperty
    @Nullable
    public String getReadId()
    {
      return readId;
    }

    /// Returns the source-controller deadline for this partition response.
    @JsonProperty
    public long getReadExpiresAtMillis()
    {
      return readExpiresAtMillis;
    }
  }

  /// Bounded status and optional manifest returned by the controller gateway.
  public static class SessionInfo
  {
    private final String queryId;
    private final String state;
    @Nullable
    private final Manifest manifest;
    @Nullable
    private final String failureCode;
    private final long leaseExpiresAtMillis;

    @JsonCreator
    public SessionInfo(
        @JsonProperty("queryId") final String queryId,
        @JsonProperty("state") final String state,
        @JsonProperty("manifest") @Nullable final Manifest manifest,
        @JsonProperty("failureCode") @Nullable final String failureCode,
        @JsonProperty("leaseExpiresAtMillis") final long leaseExpiresAtMillis
    )
    {
      this.queryId = queryId;
      this.state = state;
      this.manifest = manifest;
      this.failureCode = failureCode;
      this.leaseExpiresAtMillis = leaseExpiresAtMillis;
    }

    @JsonProperty
    public String getQueryId()
    {
      return queryId;
    }

    @JsonProperty
    public String getState()
    {
      return state;
    }

    @JsonProperty
    @Nullable
    public Manifest getManifest()
    {
      return manifest;
    }

    @JsonProperty
    @Nullable
    public String getFailureCode()
    {
      return failureCode;
    }

    @JsonProperty
    public long getLeaseExpiresAtMillis()
    {
      return leaseExpiresAtMillis;
    }
  }

  /// Stable source result metadata and opaque final-stage partition identifiers.
  public static class Manifest
  {
    private final int protocolVersion;
    private final int frameFileVersion;
    private final int finalStageNumber;
    private final RowSignature signature;
    private final Map<String, List<String>> columnMapping;
    private final List<Partition> partitions;

    @JsonCreator
    public Manifest(
        @JsonProperty("protocolVersion") final int protocolVersion,
        @JsonProperty("frameFileVersion") final int frameFileVersion,
        @JsonProperty("finalStageNumber") final int finalStageNumber,
        @JsonProperty("signature") final RowSignature signature,
        @JsonProperty("columnMapping") final Map<String, List<String>> columnMapping,
        @JsonProperty("partitions") final List<Partition> partitions
    )
    {
      this.protocolVersion = protocolVersion;
      this.frameFileVersion = frameFileVersion;
      this.finalStageNumber = finalStageNumber;
      this.signature = signature;
      final Map<String, List<String>> copiedMapping = new LinkedHashMap<>();
      if (columnMapping != null) {
        columnMapping.forEach((queryColumn, outputColumns) -> copiedMapping.put(queryColumn, List.copyOf(outputColumns)));
      }
      this.columnMapping = Map.copyOf(copiedMapping);
      this.partitions = List.copyOf(partitions);
    }

    @JsonProperty
    public int getProtocolVersion()
    {
      return protocolVersion;
    }

    @JsonProperty
    public int getFrameFileVersion()
    {
      return frameFileVersion;
    }

    @JsonProperty
    public int getFinalStageNumber()
    {
      return finalStageNumber;
    }

    @JsonProperty
    public RowSignature getSignature()
    {
      return signature;
    }

    /// Returns the physical-frame column to SQL output column mapping.
    @JsonProperty
    public Map<String, List<String>> getColumnMapping()
    {
      return columnMapping;
    }

    @JsonProperty
    public List<Partition> getPartitions()
    {
      return partitions;
    }
  }

  /// Public immutable identifier for one source worker attempt's frame partition.
  public static class Partition
  {
    private final String id;
    private final String attemptId;

    @JsonCreator
    public Partition(
        @JsonProperty("id") final String id,
        @JsonProperty("attemptId") final String attemptId
    )
    {
      this.id = id;
      this.attemptId = attemptId;
    }

    @JsonProperty
    public String getId()
    {
      return id;
    }

    @JsonProperty
    public String getAttemptId()
    {
      return attemptId;
    }
  }
}
