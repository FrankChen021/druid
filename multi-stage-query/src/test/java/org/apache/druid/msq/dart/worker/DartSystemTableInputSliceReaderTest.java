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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.msq.input.system.SystemTableSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.SystemTableRowAuthorizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class DartSystemTableInputSliceReaderTest
{
  private static final DruidNode BROKER = node("broker", 8082);
  private static final DruidNode HISTORICAL_ONE = node("historical-one", 8083);
  private static final DruidNode HISTORICAL_TWO = node("historical-two", 8084);
  private static final SystemTableDescriptor DESCRIPTOR = descriptor();

  /** A co-located source reads its provider directly, pushes down safe filters, and does not issue HTTP. */
  @Test
  public void testAttachReadsLocalProviderDirectly()
  {
    final RemoteHarness remote = new RemoteHarness();
    final SystemTableDataProvider provider = Mockito.mock(SystemTableDataProvider.class);
    Mockito.when(provider.getPushdownFilters()).thenReturn(List.of(new SystemTablePushdownFilter("server", null)));
    Mockito.when(provider.getRows(Mockito.anyList(), Mockito.any())).thenReturn(
        List.<Object[]>of(new Object[]{BROKER.getHostAndPortToUse(), "broker", "[broker]", "key", "value", null})
    );
    final SelectorDimFilter filter = new SelectorDimFilter("server", BROKER.getHostAndPortToUse(), null);

    final PhysicalInputSlice inputSlice = reader(remote, Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider)).attach(
        0,
        slice(List.of(source(BROKER, NodeRole.BROKER)), filter, List.of("server"), Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertEquals(List.of(List.of(BROKER.getHostAndPortToUse())), readRows(inputSlice, List.of("server")));
    final ArgumentCaptor<List<org.apache.druid.query.filter.DimFilter>> filters = ArgumentCaptor.forClass(List.class);
    Mockito.verify(provider).getRows(filters.capture(), Mockito.any());
    Assertions.assertEquals(List.of(filter), filters.getValue());
    Assertions.assertTrue(remote.requests.isEmpty());
  }

  /** A remote source is read through a generic system-table scan on {@code /druid/v2/}. */
  @Test
  public void testAttachReadsRemoteNativeScan() throws IOException
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.respond(
        List.of(
            new ScanResultValue(
                null,
                List.of("server", "server_type", "service", "property", "value", "error_message"),
                List.of(
                    Arrays.asList(
                        HISTORICAL_ONE.getHostAndPortToUse(),
                        "historical",
                        "[historical]",
                        "key",
                        "value",
                        null
                    )
                )
            )
        )
    );

    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertEquals(
        List.of(List.of(HISTORICAL_ONE.getHostAndPortToUse(), "key", "value")),
        readRows(inputSlice, List.of("server", "property", "value"))
    );
    Assertions.assertEquals(1, remote.requests.size());
    final Request request = remote.requests.get(0).build(ServiceLocation.fromDruidNode(HISTORICAL_ONE));
    Assertions.assertEquals("/druid/v2/", request.getUrl().getPath());
    Assertions.assertTrue(
        request.getHeaders().get(QueryResource.HEADER_NATIVE_QUERY_ROUTE)
               .contains(QueryResource.NATIVE_QUERY_ROUTE_LOCAL)
    );
    final ScanQuery query = remote.smileMapper.readValue(request.getContent().array(), ScanQuery.class);
    Assertions.assertEquals(
        new SystemTableDataSource(ServerPropertiesTableDescriptor.TABLE_NAME),
        query.getDataSource()
    );
    Assertions.assertNotNull(query.getId());
  }

  /** A row-count-only stage asks the remote node for one column, because an empty column list means all columns. */
  @Test
  public void testEmptyProjectionRequestsSingleColumnFromRemoteNode() throws IOException
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.respond(
        List.of(new ScanResultValue(null, List.of("server"), List.of(List.of(HISTORICAL_ONE.getHostAndPortToUse()))))
    );

    consume(
        reader(remote, Map.of()).attach(
            0,
            slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, List.of(), Long.MAX_VALUE),
            new CounterTracker(false),
            ignored -> {
            }
        )
    );

    final ScanQuery query = remote.smileMapper.readValue(
        remote.requests.get(0).build(ServiceLocation.fromDruidNode(HISTORICAL_ONE)).getContent().array(),
        ScanQuery.class
    );
    Assertions.assertEquals(List.of("server"), query.getColumns());
  }

  /** All remote native-query requests start before the reader waits for the first response. */
  @Test
  @Timeout(30)
  public void testAttachStartsRemoteRequestsTogether() throws IOException
  {
    final RemoteHarness remote = new RemoteHarness();
    final SettableFuture<byte[]> firstResponse = SettableFuture.create();
    final AtomicInteger requestsStarted = new AtomicInteger();
    remote.responses.add(firstResponse);
    remote.responses.add(Futures.immediateFuture(remote.responseBytes("second")));
    remote.onRequest = () -> {
      if (requestsStarted.incrementAndGet() == 2) {
        try {
          firstResponse.set(remote.responseBytes("first"));
        }
        catch (IOException e) {
          firstResponse.setException(e);
        }
      }
    };

    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(
            List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL), source(HISTORICAL_TWO, NodeRole.HISTORICAL)),
            null,
            null,
            Long.MAX_VALUE
        ),
        new CounterTracker(false),
        ignored -> {
        }
    );

    // Rows are emitted in source order even though the second response is ready first.
    Assertions.assertEquals(
        List.of(List.of("first"), List.of("second")),
        readRows(inputSlice, List.of("property"))
    );
    Assertions.assertEquals(2, requestsStarted.get());
  }

  /** Authorization and other non-availability endpoint failures fail the query rather than becoming data rows. */
  @Test
  public void testAttachPropagatesEndpointFailure()
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.fail(new RpcException("forbidden"));
    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    final RE exception = Assertions.assertThrows(
        RE.class,
        () -> consume(inputSlice)
    );
    Assertions.assertInstanceOf(RpcException.class, exception.getCause());
    Assertions.assertEquals("forbidden", exception.getCause().getMessage());
  }

  /** A co-located source applies the slice limit only when no residual filter could discard rows afterwards. */
  @Test
  public void testLocalLimitIsAppliedOnlyWithoutResidualFilter()
  {
    final SystemTableDataProvider provider = Mockito.mock(SystemTableDataProvider.class);
    Mockito.when(provider.getRows(Mockito.anyList(), Mockito.any())).thenAnswer(
        invocation -> List.<Object[]>of(row("a"), row("b"), row("c"))
    );
    final Map<String, SystemTableDataProvider> providers = Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider);
    final List<String> columns = List.of("property");

    Assertions.assertEquals(
        List.of(List.of("a"), List.of("b")),
        readRows(
            reader(new RemoteHarness(), providers).attach(
                0,
                slice(List.of(source(BROKER, NodeRole.BROKER)), null, columns, 2),
                new CounterTracker(false),
                ignored -> {
                }
            ),
            columns
        )
    );
    Assertions.assertEquals(
        List.of(List.of("a"), List.of("b"), List.of("c")),
        readRows(
            reader(new RemoteHarness(), providers).attach(
                0,
                slice(
                    List.of(source(BROKER, NodeRole.BROKER)),
                    new SelectorDimFilter("property", "b", null),
                    columns,
                    2
                ),
                new CounterTracker(false),
                ignored -> {
                }
            ),
            columns
        )
    );
  }

  /** Rows read from a co-located provider pass through the descriptor's row authorizer. */
  @Test
  public void testLocalRowsAreFilteredByRowAuthorizer()
  {
    final SystemTableDataProvider provider = Mockito.mock(SystemTableDataProvider.class);
    Mockito.when(provider.getRows(Mockito.anyList(), Mockito.any())).thenReturn(
        List.<Object[]>of(row("allowed"), row("denied"))
    );
    final SystemTableDescriptor descriptor = descriptor(
        (rows, authenticationResult, authorizerMapper) -> {
          final List<Object[]> authorized = new ArrayList<>();
          for (final Object[] row : rows) {
            if (!"denied".equals(row[3])) {
              authorized.add(row);
            }
          }
          return authorized;
        }
    );

    final List<String> columns = List.of("property");
    final PhysicalInputSlice inputSlice = reader(
        new RemoteHarness(),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider),
        descriptor
    ).attach(
        0,
        slice(List.of(source(BROKER, NodeRole.BROKER)), null, columns, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertEquals(List.of(List.of("allowed")), readRows(inputSlice, columns));
  }

  /** A node transport failure is represented by the descriptor's availability row. */
  @Test
  public void testAttachRecoversNodeAvailabilityFailure()
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.fail(new SocketException("connection refused"));

    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(
            List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)),
            null,
            List.of("server", "error_message"),
            Long.MAX_VALUE
        ),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertEquals(
        List.of(List.of(HISTORICAL_ONE.getHostAndPortToUse(), "connection refused")),
        readRows(inputSlice, List.of("server", "error_message"))
    );
  }

  /** A remote native-query timeout retains query-timeout semantics rather than becoming an availability row. */
  @Test
  public void testAttachPropagatesRemoteTimeoutAsQueryTimeout()
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.fail(new ReadTimeoutException());
    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertThrows(QueryTimeoutException.class, () -> consume(inputSlice));
  }

  /** A request added concurrently after slice cleanup is cancelled and its closer is closed immediately. */
  @Test
  public void testRemoteRequestTrackerCancelsRequestAddedAfterClose() throws IOException
  {
    final DartSystemTableInputSliceReader.RemoteRequestTracker tracker =
        new DartSystemTableInputSliceReader.RemoteRequestTracker();
    final SettableFuture<Object> request = SettableFuture.create();
    final org.apache.druid.java.util.common.io.Closer closer = Mockito.spy(
        org.apache.druid.java.util.common.io.Closer.create()
    );

    tracker.cancelAll();
    tracker.track(request, closer);

    Assertions.assertTrue(request.isCancelled());
    Mockito.verify(closer).close();
  }

  /** Closing a partially consumed slice cancels every in-flight request. */
  @Test
  public void testClosingActiveSliceCancelsAllRemoteRequests() throws Exception
  {
    final RemoteHarness remote = new RemoteHarness();
    remote.responses.add(Futures.immediateFuture(remote.responseBytes("first")));
    final SettableFuture<byte[]> pendingResponse = SettableFuture.create();
    remote.responses.add(pendingResponse);
    final PhysicalInputSlice inputSlice = reader(remote, Map.of()).attach(
        0,
        slice(
            List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL), source(HISTORICAL_TWO, NodeRole.HISTORICAL)),
            null,
            null,
            Long.MAX_VALUE
        ),
        new CounterTracker(false),
        ignored -> {
        }
    );

    final AcquireSegmentAction action = inputSlice.getLoadableSegments().get(0).acquire(AcquireMode.FULL);
    try (action) {
      action.await();
      try (AcquireSegmentResult acquireResult = action.release()) {
        final Segment segment = acquireResult.getSegment().orElseThrow();
        try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          final Cursor cursor = holder.asCursor();
          Assertions.assertFalse(cursor.isDone());
          Assertions.assertFalse(pendingResponse.isCancelled());
        }
      }
    }

    Assertions.assertTrue(pendingResponse.isCancelled());
    // Node-local scans are not registered for explicit cancellation; aborting the request is what stops the node.
    Assertions.assertTrue(remote.cancellationRequests.isEmpty());
  }

  /** One request that fails to clean up must not prevent the remaining requests of the slice from being cancelled. */
  @Test
  public void testRemoteRequestTrackerCancelsAllRequestsWhenOneCloseFails() throws IOException
  {
    final DartSystemTableInputSliceReader.RemoteRequestTracker tracker =
        new DartSystemTableInputSliceReader.RemoteRequestTracker();
    final SettableFuture<Object> firstRequest = SettableFuture.create();
    final SettableFuture<Object> secondRequest = SettableFuture.create();
    final org.apache.druid.java.util.common.io.Closer failingCloser = org.apache.druid.java.util.common.io.Closer.create();
    failingCloser.register(() -> {
      throw new IOException("close failed");
    });
    final org.apache.druid.java.util.common.io.Closer secondCloser = Mockito.spy(
        org.apache.druid.java.util.common.io.Closer.create()
    );
    tracker.track(firstRequest, failingCloser);
    tracker.track(secondRequest, secondCloser);

    Assertions.assertThrows(RE.class, tracker::cancelAll);

    Assertions.assertTrue(firstRequest.isCancelled());
    Assertions.assertTrue(secondRequest.isCancelled());
    Mockito.verify(secondCloser).close();
  }

  private static DartSystemTableInputSliceReader reader(
      final RemoteHarness remote,
      final Map<String, SystemTableDataProvider> providers
  )
  {
    return reader(remote, providers, DESCRIPTOR);
  }

  private static DartSystemTableInputSliceReader reader(
      final RemoteHarness remote,
      final Map<String, SystemTableDataProvider> providers,
      final SystemTableDescriptor descriptor
  )
  {
    return new DartSystemTableInputSliceReader(
        remote.serviceClientFactory,
        remote.smileMapper,
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, descriptor),
        providers,
        BROKER,
        Mockito.mock(AuthenticationResult.class),
        Mockito.mock(AuthorizerMapper.class),
        QueryContext.empty()
    );
  }

  private static SystemTableInputSlice slice(
      final List<SystemTableSource> sources,
      final org.apache.druid.query.filter.DimFilter filter,
      final List<String> columns,
      final long limit
  )
  {
    return new SystemTableInputSlice(
        ServerPropertiesTableDescriptor.TABLE_NAME,
        sources,
        filter,
        columns,
        VirtualColumns.EMPTY,
        limit
    );
  }

  private static SystemTableSource source(final DruidNode node, final NodeRole role)
  {
    return new SystemTableSource(node, Set.of(role));
  }

  private static SystemTableDescriptor descriptor()
  {
    return descriptor((rows, authenticationResult, authorizerMapper) -> rows);
  }

  private static SystemTableDescriptor descriptor(final SystemTableRowAuthorizer rowAuthorizer)
  {
    final ServerPropertiesTableDescriptor delegate = new ServerPropertiesTableDescriptor();
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getRowSignature()).thenReturn(ServerPropertiesTableDescriptor.ROW_SIGNATURE);
    Mockito.when(descriptor.getRowAuthorizer()).thenReturn(rowAuthorizer);
    Mockito.when(descriptor.getNodeFailureRow(Mockito.any(), Mockito.anySet(), Mockito.any())).thenAnswer(
        invocation -> delegate.getNodeFailureRow(
            invocation.getArgument(0),
            invocation.getArgument(1),
            invocation.getArgument(2)
        )
    );
    return descriptor;
  }

  private static Object[] row(final String property)
  {
    return new Object[]{BROKER.getHostAndPortToUse(), "broker", "[broker]", property, "value", null};
  }

  private static void consume(final PhysicalInputSlice physicalInputSlice)
  {
    final AcquireSegmentAction action = physicalInputSlice.getLoadableSegments().get(0).acquire(AcquireMode.FULL);
    try (action) {
      action.await();
      try (AcquireSegmentResult acquireResult = action.release()) {
        final Segment segment = acquireResult.getSegment().orElseThrow();
        try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          final Cursor cursor = holder.asCursor();
          while (cursor != null && !cursor.isDone()) {
            cursor.advance();
          }
        }
      }
    }
    catch (Exception e) {
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RuntimeException(e);
    }
  }

  private static List<List<Object>> readRows(
      final PhysicalInputSlice physicalInputSlice,
      final List<String> columns
  )
  {
    final List<List<Object>> rows = new ArrayList<>();
    final AcquireSegmentAction action = physicalInputSlice.getLoadableSegments().get(0).acquire(AcquireMode.FULL);
    try (action) {
      action.await();
      try (AcquireSegmentResult acquireResult = action.release()) {
        final Segment segment = acquireResult.getSegment().orElseThrow();
        try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          final Cursor cursor = holder.asCursor();
          if (cursor == null) {
            return rows;
          }
          final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
          final List<ColumnValueSelector<?>> selectors = new ArrayList<>();
          for (final String column : columns) {
            selectors.add(selectorFactory.makeColumnValueSelector(column));
          }
          while (!cursor.isDone()) {
            final List<Object> row = new ArrayList<>(selectors.size());
            for (final ColumnValueSelector<?> selector : selectors) {
              row.add(selector.getObject());
            }
            rows.add(row);
            cursor.advance();
          }
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return rows;
  }

  private static DruidNode node(final String service, final int port)
  {
    return new DruidNode(service, "localhost", false, port, -1, true, false);
  }

  private static class RemoteHarness
  {
    private final ObjectMapper smileMapper = new org.apache.druid.jackson.DefaultObjectMapper(new SmileFactory(), null);
    private final Queue<ListenableFuture<byte[]>> responses = new ArrayDeque<>();
    private final List<RequestBuilder> requests = new ArrayList<>();
    private final List<RequestBuilder> cancellationRequests = new ArrayList<>();
    private final ServiceClient serviceClient = new ServiceClient()
    {
      @Override
      public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
          final RequestBuilder requestBuilder,
          final HttpResponseHandler<IntermediateType, FinalType> handler
      )
      {
        final Request request = requestBuilder.build(ServiceLocation.fromDruidNode(HISTORICAL_ONE));
        if (HttpMethod.DELETE.equals(request.getMethod())) {
          cancellationRequests.add(requestBuilder);
          return Futures.immediateFuture(null);
        }
        requests.add(requestBuilder);
        onRequest.run();
        final ListenableFuture<byte[]> response = responses.remove();
        return Futures.transform(response, bytes -> handleResponse(handler, bytes), Runnable::run);
      }

      @Override
      public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
      {
        return this;
      }
    };
    private final ServiceClientFactory serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> serviceClient;
    private Runnable onRequest = () -> {
    };

    private void respond(final List<ScanResultValue> response) throws IOException
    {
      responses.add(Futures.immediateFuture(smileMapper.writeValueAsBytes(response)));
    }

    private byte[] responseBytes(final String value) throws IOException
    {
      return smileMapper.writeValueAsBytes(
          List.of(
              new ScanResultValue(
                  null,
                  List.of("server", "server_type", "service", "property", "value", "error_message"),
                  List.of(
                      Arrays.asList(
                          HISTORICAL_ONE.getHostAndPortToUse(),
                          "historical",
                          "[historical]",
                          value,
                          value,
                          null
                      )
                  )
              )
          )
      );
    }

    private void fail(final Throwable throwable)
    {
      responses.add(Futures.immediateFailedFuture(throwable));
    }

    private static <IntermediateType, FinalType> FinalType handleResponse(
        final HttpResponseHandler<IntermediateType, FinalType> handler,
        final byte[] bytes
    )
    {
      ClientResponse<IntermediateType> response = handler.handleResponse(
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
          new HttpResponseHandler.TrafficCop()
          {
            @Override
            public long resume(final long chunkNum)
            {
              return 0;
            }

            @Override
            public void abort()
            {
            }
          }
      );
      response = handler.handleChunk(response, new DefaultHttpContent(Unpooled.wrappedBuffer(bytes)), 1);
      return handler.done(response).getObj();
    }
  }
}
