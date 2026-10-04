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

package org.apache.druid.msq.input.external;

import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.RemoteDruidFrameInputSource;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.query.filter.SegmentPruner;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/// Submits a remote source query once, assigns its manifest partitions, and releases its lease on close.
public class RemoteDruidFrameInputSpecSlicer implements InputSpecSlicer, Closeable
{
  private static final Logger LOG = new Logger(RemoteDruidFrameInputSpecSlicer.class);
  private static final long LEASE_RENEWAL_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(30);

  private final ExternalInputSpecSlicer delegate = new ExternalInputSpecSlicer();
  @Nullable
  private RemoteDruidFrameInputSource source;
  @Nullable
  private List<InputSource> partitionSources;
  @Nullable
  private String sessionId;
  @Nullable
  private ScheduledExecutorService leaseRenewalExecutor;
  private boolean closed;

  @Override
  public boolean canSliceDynamic(final InputSpec inputSpec)
  {
    return isRemote(inputSpec) || delegate.canSliceDynamic(inputSpec);
  }

  @Override
  public List<InputSlice> sliceStatic(
      final InputSpec inputSpec,
      @Nullable final SegmentPruner segmentPruner,
      final int maxNumSlices
  )
  {
    if (!isRemote(inputSpec)) {
      return delegate.sliceStatic(inputSpec, segmentPruner, maxNumSlices);
    }
    return sliceRemote((ExternalInputSpec) inputSpec, maxNumSlices);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      final InputSpec inputSpec,
      @Nullable final SegmentPruner segmentPruner,
      final int maxNumSlices,
      final int maxFilesPerSlice,
      final long maxBytesPerSlice
  )
  {
    if (!isRemote(inputSpec)) {
      return delegate.sliceDynamic(inputSpec, segmentPruner, maxNumSlices, maxFilesPerSlice, maxBytesPerSlice);
    }
    return sliceRemote((ExternalInputSpec) inputSpec, maxNumSlices);
  }

  private static boolean isRemote(final InputSpec inputSpec)
  {
    return inputSpec instanceof ExternalInputSpec
           && ((ExternalInputSpec) inputSpec).getInputSource() instanceof RemoteDruidFrameInputSource;
  }

  private synchronized List<InputSlice> sliceRemote(final ExternalInputSpec spec, final int maxNumSlices)
  {
    if (closed) {
      throw new IllegalStateException("Remote frame input slicer is already closed");
    }
    if (maxNumSlices < 1) {
      throw new IllegalArgumentException("maxNumSlices must be positive");
    }

    final RemoteDruidFrameInputSource inputSource = (RemoteDruidFrameInputSource) spec.getInputSource();
    if (source == null) {
      source = inputSource;
      try {
        final List<InputSplit<RemoteDruidFrameInputSource.PartitionSplit>> splits = inputSource.createSplits(
            spec.getInputFormat(),
            null
        ).collect(Collectors.toList());
        partitionSources = splits.stream()
                                 .map(inputSource::withSplit)
                                 .collect(Collectors.toList());
        if (!splits.isEmpty()) {
          sessionId = splits.get(0).get().getSessionId();
          startLeaseRenewal();
        }
      }
      catch (IOException e) {
        throw new RuntimeException("Could not submit the remote MSQ source query", e);
      }
    } else if (source != inputSource) {
      throw new IllegalStateException("One target controller cannot own multiple remote source sessions");
    }

    if (partitionSources.isEmpty()) {
      return List.of(NilInputSlice.INSTANCE);
    }

    final int numSlices = Math.min(maxNumSlices, partitionSources.size());
    final List<List<InputSource>> slices = new ArrayList<>(numSlices);
    for (int i = 0; i < numSlices; i++) {
      slices.add(new ArrayList<>());
    }
    for (int i = 0; i < partitionSources.size(); i++) {
      slices.get(i % numSlices).add(partitionSources.get(i));
    }
    return slices.stream()
                 .map(inputSources -> new ExternalInputSlice(inputSources, spec.getInputFormat(), spec.getSignature()))
                 .collect(Collectors.toList());
  }

  private void startLeaseRenewal()
  {
    leaseRenewalExecutor = Execs.scheduledSingleThreaded("remote-frame-lease-%d");
    leaseRenewalExecutor.scheduleAtFixedRate(
        () -> {
          try {
            source.renewSession(sessionId, RemoteDruidFrameInputSource.LEASE_MILLIS);
          }
          catch (IOException e) {
            LOG.noStackTrace().warn(e, "Could not renew a remote frame source lease");
          }
        },
        LEASE_RENEWAL_INTERVAL_MILLIS,
        LEASE_RENEWAL_INTERVAL_MILLIS,
        TimeUnit.MILLISECONDS
    );
  }

  @Override
  public synchronized void close() throws IOException
  {
    if (closed) {
      return;
    }
    closed = true;
    if (leaseRenewalExecutor != null) {
      leaseRenewalExecutor.shutdownNow();
    }
    if (source != null && sessionId != null) {
      source.releaseSession(sessionId);
    }
  }
}
