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

package org.apache.druid.msq.dart.controller;

import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.input.system.SystemTableSource;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.server.system.handler.SystemTableNode;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.table.SystemTableDescriptor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Assigns each system-table source to a co-located worker, or to the Broker worker as a proxy fallback. */
public class DartSystemTableInputSpecSlicer implements InputSpecSlicer
{
  private final SystemTableNodeLocator nodeLocator;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final List<String> workerIds;
  private final long queryFailTime;

  public DartSystemTableInputSpecSlicer(
      final SystemTableNodeLocator nodeLocator,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final List<String> workerIds,
      final long queryFailTime
  )
  {
    this.nodeLocator = nodeLocator;
    this.tableDescriptors = tableDescriptors;
    this.workerIds = workerIds;
    this.queryFailTime = queryFailTime;
  }

  @Override
  public boolean canSliceDynamic(final InputSpec inputSpec)
  {
    return false;
  }

  @Override
  public List<InputSlice> sliceStatic(
      final InputSpec inputSpec,
      @Nullable final SegmentPruner segmentPruner,
      final int maxNumSlices
  )
  {
    final SystemTableInputSpec systemTableInputSpec = (SystemTableInputSpec) inputSpec;
    final SystemTableDescriptor descriptor = tableDescriptors.get(systemTableInputSpec.getTable());
    if (descriptor == null) {
      throw new IllegalStateException(
          "No descriptor is registered for system table[" + systemTableInputSpec.getTable() + "]"
      );
    }

    // Production Dart workers are known before slicing. Test and alternate controller contexts may create workers
    // lazily, in which case reserve slice zero for the controller-selected fallback worker.
    final int sliceCount = Math.min(maxNumSlices, Math.max(1, workerIds.size()));
    final List<List<SystemTableSource>> sourcesByWorker = new ArrayList<>(sliceCount);
    final Map<String, Integer> workerNumberByHost = new HashMap<>();
    for (int i = 0; i < sliceCount; i++) {
      sourcesByWorker.add(new ArrayList<>());
      if (i < workerIds.size()) {
        workerNumberByHost.put(WorkerId.fromString(workerIds.get(i)).getHostAndPort(), i);
      }
    }

    for (final SystemTableNode node : nodeLocator.locate(descriptor, queryFailTime)) {
      final SystemTableSource source = new SystemTableSource(
          node.getDiscoveryNode().getDruidNode(),
          node.getNodeRoles()
      );
      // Worker zero is always the Broker. It proxies nodes without an active co-located Dart worker.
      sourcesByWorker.get(workerNumberByHost.getOrDefault(source.getNode().getHostAndPortToUse(), 0)).add(source);
    }

    final List<InputSlice> slices = new ArrayList<>(sliceCount);
    for (final List<SystemTableSource> sources : sourcesByWorker) {
      if (sources.isEmpty()) {
        slices.add(NilInputSlice.INSTANCE);
      } else {
        slices.add(
            new SystemTableInputSlice(
                systemTableInputSpec.getTable(),
                sources,
                systemTableInputSpec.getFilter(),
                systemTableInputSpec.getColumns(),
                systemTableInputSpec.getVirtualColumns(),
                systemTableInputSpec.getLimit()
            )
        );
      }
    }
    return slices;
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
    throw new UnsupportedOperationException();
  }
}
