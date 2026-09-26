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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
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
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.SystemTableNodeFailure;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/** Reads assigned system-table sources as a lazy row segment for a Dart stage. */
public class DartSystemTableInputSliceReader implements InputSliceReader
{
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final Map<String, SystemTableDataProvider> dataProviders;
  private final DruidNode selfNode;
  private final AuthenticationResult escalatedAuthenticationResult;
  private final AuthorizerMapper authorizerMapper;
  private final QueryContext queryContext;
  private final List<ListenableFuture<?>> remoteRequests = new ArrayList<>();

  public DartSystemTableInputSliceReader(
      final HttpClient httpClient,
      final ObjectMapper jsonMapper,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final Map<String, SystemTableDataProvider> dataProviders,
      final DruidNode selfNode,
      final AuthenticationResult escalatedAuthenticationResult,
      final AuthorizerMapper authorizerMapper,
      final QueryContext queryContext
  )
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
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
    final Sequence<Object[]> rows = Sequences.withBaggage(
        new LazySequence<>(() -> makeRows(systemTableSlice, descriptor, projection)),
        (Closeable) this::cancelRemoteRequests
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
      final Projection projection
  )
  {
    final List<Sequence<Object[]>> sourceSequences = new ArrayList<>();
    for (final SystemTableSource source : slice.getSources()) {
      if (SystemTableNodeLocator.sameServer(selfNode.getUriToUse(), source.getNode().getUriToUse())) {
        sourceSequences.add(localRows(slice, descriptor, projection));
      } else {
        if (!ServerPropertiesTableDescriptor.TABLE_NAME.equals(slice.getTable())) {
          throw new IllegalStateException("Remote Dart system-table reads are not implemented for sys." + slice.getTable());
        }
        final ListenableFuture<StringFullResponseHolder> request = startRemoteRequest(source);
        remoteRequests.add(request);
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
      final ListenableFuture<StringFullResponseHolder> request
  )
  {
    try {
      final StringFullResponseHolder response = request.get();
      if (response.getStatus().code() != HttpServletResponse.SC_OK) {
        throw new RE(
            "Node[%s] returned HTTP status[%s] while reading sys.%s",
            source.getNode().getHostAndPortToUse(),
            response.getStatus(),
            slice.getTable()
        );
      }
      final Map<String, String> properties = new TreeMap<>(
          jsonMapper.readValue(response.getContent(), new TypeReference<Map<String, String>>() {})
      );
      final List<Object[]> rows = new ArrayList<>();
      final String nodeRoles = source.getNodeRoles()
                                     .stream()
                                     .map(role -> role.getJsonName())
                                     .sorted()
                                     .toList()
                                     .toString();
      if (properties.isEmpty()) {
        rows.add(projection.apply(serverPropertiesRow(source, nodeRoles, null, null, null)));
      } else {
        properties.forEach(
            (property, value) -> rows.add(
                projection.apply(serverPropertiesRow(source, nodeRoles, property, value, null))
            )
        );
      }
      final Sequence<Object[]> sequence = Sequences.simple(rows);
      return slice.getFilter() == null ? sequence.limit(slice.getLimit()) : sequence;
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

  private ListenableFuture<StringFullResponseHolder> startRemoteRequest(final SystemTableSource source)
  {
    try {
      final URL url = source.getNode().getUriToUse().resolve("/status/properties").toURL();
      return httpClient.go(
          new Request(HttpMethod.GET, url),
          new StringFullResponseHandler(StandardCharsets.UTF_8),
          remainingTimeout(queryContext)
      );
    }
    catch (Exception e) {
      throw new RE(e, "Unable to request sys.server_properties from node[%s]", source.getNode());
    }
  }

  private static Sequence<Object[]> recoverRemoteFailure(
      final SystemTableSource source,
      final SystemTableDescriptor descriptor,
      final Projection projection,
      final Throwable failure
  )
  {
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

  private static Object[] serverPropertiesRow(
      final SystemTableSource source,
      final String nodeRoles,
      final String property,
      final String value,
      final String error
  )
  {
    return new Object[]{
        source.getNode().getHostAndPortToUse(),
        source.getNode().getServiceName(),
        nodeRoles,
        property,
        value,
        error
    };
  }

  private void cancelRemoteRequests()
  {
    remoteRequests.forEach(request -> request.cancel(true));
    remoteRequests.clear();
  }

  private static Duration remainingTimeout(final QueryContext queryContext)
  {
    DateTime deadline = MultiStageQueryContext.getQueryDeadline(queryContext);
    if (deadline == null) {
      final long timeout = queryContext.getTimeout(QueryContexts.NO_TIMEOUT);
      if (timeout == QueryContexts.NO_TIMEOUT) {
        return null;
      }
      deadline = MultiStageQueryContext.getStartTime(queryContext).plus(timeout);
    }
    return Duration.millis(Math.max(1, deadline.getMillis() - System.currentTimeMillis()));
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
}
