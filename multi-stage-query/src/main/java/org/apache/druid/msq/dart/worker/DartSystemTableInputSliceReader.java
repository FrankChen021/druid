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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.AdaptedLoadableSegment;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.msq.input.system.SystemTableSource;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.SystemTableNodeFailure;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** Reads assigned system-table sources as a lazy row segment for a Dart stage. */
public class DartSystemTableInputSliceReader implements InputSliceReader
{
  private final ServiceClientFactory serviceClientFactory;
  private final ObjectMapper smileMapper;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final Map<String, SystemTableDataProvider> dataProviders;
  private final DruidNode selfNode;
  private final AuthenticationResult escalatedAuthenticationResult;
  private final AuthorizerMapper authorizerMapper;
  private final QueryContext queryContext;

  public DartSystemTableInputSliceReader(
      final ServiceClientFactory serviceClientFactory,
      final ObjectMapper smileMapper,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final Map<String, SystemTableDataProvider> dataProviders,
      final DruidNode selfNode,
      final AuthenticationResult escalatedAuthenticationResult,
      final AuthorizerMapper authorizerMapper,
      final QueryContext queryContext
  )
  {
    this.serviceClientFactory = serviceClientFactory;
    this.smileMapper = smileMapper;
    this.tableDescriptors = tableDescriptors;
    this.dataProviders = dataProviders;
    this.selfNode = selfNode;
    this.escalatedAuthenticationResult = escalatedAuthenticationResult;
    this.authorizerMapper = authorizerMapper;
    this.queryContext = queryContext;
  }

  @Override
  public PhysicalInputSlice attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final SystemTableInputSlice systemTableSlice = (SystemTableInputSlice) slice;
    final SystemTableDescriptor descriptor = tableDescriptors.get(systemTableSlice.getTable());
    if (descriptor == null) {
      throw new IllegalStateException("No descriptor is registered for system table[" + systemTableSlice.getTable() + "]");
    }

    final Projection projection = makeProjection(descriptor.getRowSignature(), systemTableSlice.getColumns());
    final RemoteRequestTracker remoteRequestTracker = new RemoteRequestTracker();
    final Sequence<Object[]> rows = Sequences.withBaggage(
        new LazySequence<>(() -> makeRows(systemTableSlice, descriptor, projection, remoteRequestTracker)),
        (Closeable) remoteRequestTracker::cancelAll
    );
    final InlineDataSource rowAdapterSource = InlineDataSource.fromIterable(List.of(), projection.signature());
    final Segment segment = new RowBasedSegment<>(rows, rowAdapterSource.rowAdapter(), projection.signature());
    final LoadableSegment loadableSegment = AdaptedLoadableSegment.fromUnmanagedSegment(
        segment,
        new SegmentDescriptor(Intervals.ETERNITY, "0", 0),
        "system table " + systemTableSlice.getTable(),
        counters.channel(CounterNames.inputChannel(inputNumber))
    );
    return new PhysicalInputSlice(
        ReadablePartitions.empty(),
        Collections.singletonList(loadableSegment),
        Collections.emptyList()
    );
  }

  private Sequence<Object[]> makeRows(
      final SystemTableInputSlice slice,
      final SystemTableDescriptor descriptor,
      final Projection projection,
      final RemoteRequestTracker remoteRequestTracker
  )
  {
    final List<Sequence<Object[]>> sourceSequences = new ArrayList<>();
    for (final SystemTableSource source : slice.getSources()) {
      if (SystemTableNodeLocator.sameServer(selfNode.getUriToUse(), source.getNode().getUriToUse())) {
        sourceSequences.add(localRows(slice, descriptor, projection));
      } else {
        final Closer closer = Closer.create();
        final ListenableFuture<Sequence<ScanResultValue>> request = startRemoteRequest(slice, source, projection, closer);
        remoteRequestTracker.track(request, closer);
        sourceSequences.add(new LazySequence<>(() -> remoteRows(slice, source, descriptor, projection, request)));
      }
    }
    return Sequences.concat(sourceSequences);
  }

  private Sequence<Object[]> localRows(
      final SystemTableInputSlice slice,
      final SystemTableDescriptor descriptor,
      final Projection projection
  )
  {
    final SystemTableDataProvider provider = dataProviders.get(slice.getTable());
    if (provider == null) {
      throw new IllegalStateException("System table[" + slice.getTable() + "] is not served by this worker node");
    }
    final Iterable<Object[]> suppliedRows = () -> provider.getRows(
        SystemTablePushdownFilter.extract(slice.getFilter(), provider.getPushdownFilters()),
        escalatedAuthenticationResult
    ).iterator();
    final Iterable<Object[]> authorizedRows = descriptor.getRowAuthorizer().filterAuthorizedRows(
        suppliedRows,
        escalatedAuthenticationResult,
        authorizerMapper
    );
    final Sequence<Object[]> rows = Sequences.simple(authorizedRows).map(projection::apply);
    // A provider pushdown may be only a subset of the Druid filter. Limit before the residual filter is only safe when
    // there is no residual predicate at all.
    return slice.getFilter() == null ? rows.limit(slice.getLimit()) : rows;
  }

  private Sequence<Object[]> remoteRows(
      final SystemTableInputSlice slice,
      final SystemTableSource source,
      final SystemTableDescriptor descriptor,
      final Projection projection,
      final ListenableFuture<Sequence<ScanResultValue>> request
  )
  {
    try {
      return request.get().flatMap(DartSystemTableInputSliceReader::rowsFromScanResult);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RE(e, "Interrupted while reading sys.%s from node[%s]", slice.getTable(), source.getNode());
    }
    catch (ExecutionException e) {
      return recoverRemoteFailure(source, descriptor, projection, e.getCause());
    }
    catch (CancellationException e) {
      throw e;
    }
    catch (Exception e) {
      return recoverRemoteFailure(source, descriptor, projection, e);
    }
  }

  private ListenableFuture<Sequence<ScanResultValue>> startRemoteRequest(
      final SystemTableInputSlice slice,
      final SystemTableSource source,
      final Projection projection,
      final Closer closer
  )
  {
    try {
      final ScanQuery query = Druids.newScanQueryBuilder()
                                    .dataSource(new SystemTableDataSource(slice.getTable()))
                                    .eternityInterval()
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .filters(slice.getFilter())
                                    .virtualColumns(slice.getVirtualColumns())
                                    .columns(projection.signature())
                                    .limit(slice.getLimit())
                                    .context(
                                        BaseQuery.computeOverriddenContext(
                                            queryContext.asMap(),
                                            Map.of(BaseQuery.QUERY_ID, UUID.randomUUID().toString())
                                        )
                                    )
                                    .build();
      final DataServerClient client = new DataServerClient(
          serviceClientFactory,
          ServiceLocation.fromDruidNode(source.getNode()),
          smileMapper,
          StandardRetryPolicy.noRetries(),
          true
      );
      final JavaType resultType = smileMapper.getTypeFactory().constructType(ScanResultValue.class);
      return client.run(query, new DefaultResponseContext(), resultType, closer);
    }
    catch (Exception e) {
      if (isTimeoutFailure(e)) {
        throw timeoutException(source);
      }
      throw new RE(e, "Unable to request sys.%s from node[%s]", slice.getTable(), source.getNode());
    }
  }

  private static Sequence<Object[]> rowsFromScanResult(final ScanResultValue scanResult)
  {
    return Sequences.simple((List<?>) scanResult.getEvents())
                    .map(event -> event instanceof Object[] ? (Object[]) event : ((List<?>) event).toArray());
  }

  private static Sequence<Object[]> recoverRemoteFailure(
      final SystemTableSource source,
      final SystemTableDescriptor descriptor,
      final Projection projection,
      final Throwable failure
  )
  {
    if (isTimeoutFailure(failure)) {
      if (failure instanceof QueryTimeoutException queryTimeoutException) {
        throw queryTimeoutException;
      }
      throw timeoutException(source);
    }
    if (!SystemTableNodeFailure.isAvailabilityFailure(failure)) {
      throw failure instanceof RuntimeException
            ? (RuntimeException) failure
            : new RE(failure, "Failed to read system table from node[%s]", source.getNode());
    }
    final Exception exception = failure instanceof Exception
                                ? (Exception) failure
                                : new RuntimeException(failure);
    return descriptor.getNodeFailureRow(source.getNode(), source.getNodeRoles(), exception)
                     .map(row -> Sequences.simple(Collections.singletonList(projection.apply(row))))
                     .orElseGet(() -> Sequences.<Object[]>empty());
  }

  private static QueryTimeoutException timeoutException(final SystemTableSource source)
  {
    return new QueryTimeoutException(
        "Timed out while reading sys.server_properties from node[" + source.getNode().getHostAndPortToUse() + "]"
    );
  }

  private static boolean isTimeoutFailure(final Throwable failure)
  {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof QueryTimeoutException
          || current instanceof TimeoutException
          || current instanceof ReadTimeoutException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static Projection makeProjection(
      final RowSignature tableSignature,
      final List<String> requestedColumns
  )
  {
    if (requestedColumns == null) {
      final int[] indexes = new int[tableSignature.size()];
      for (int i = 0; i < indexes.length; i++) {
        indexes[i] = i;
      }
      return new Projection(tableSignature, indexes, true);
    }

    final RowSignature.Builder signature = RowSignature.builder();
    final List<Integer> indexes = new ArrayList<>();
    for (final String column : requestedColumns) {
      final int index = tableSignature.indexOf(column);
      if (index >= 0) {
        signature.add(column, tableSignature.getColumnType(index).orElse(null));
        indexes.add(index);
      }
    }
    return new Projection(signature.build(), indexes.stream().mapToInt(Integer::intValue).toArray(), false);
  }

  private record Projection(RowSignature signature, int[] indexes, boolean identity)
  {
    private Object[] apply(final Object[] row)
    {
      if (identity) {
        return row;
      }
      final Object[] projected = new Object[indexes.length];
      for (int i = 0; i < indexes.length; i++) {
        projected[i] = row[indexes[i]];
      }
      return projected;
    }
  }

  /** Tracks the requests belonging to one attached slice and closes requests that race with slice cleanup. */
  static class RemoteRequestTracker
  {
    private final List<ListenableFuture<?>> requests = new ArrayList<>();
    private final List<Closer> closers = new ArrayList<>();
    private boolean closed;

    synchronized void track(final ListenableFuture<?> request, final Closer closer)
    {
      if (closed) {
        request.cancel(true);
        close(closer);
      } else {
        requests.add(request);
        closers.add(closer);
      }
    }

    void cancelAll()
    {
      final List<ListenableFuture<?>> requestsToCancel;
      final List<Closer> closersToClose;
      synchronized (this) {
        closed = true;
        requestsToCancel = List.copyOf(requests);
        closersToClose = List.copyOf(closers);
        requests.clear();
        closers.clear();
      }
      requestsToCancel.forEach(request -> request.cancel(true));
      closersToClose.forEach(RemoteRequestTracker::close);
    }

    private static void close(final Closer closer)
    {
      try {
        closer.close();
      }
      catch (IOException e) {
        throw new RE(e, "Unable to close a remote system-table request");
      }
    }
  }
}
