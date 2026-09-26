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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.msq.input.system.SystemTableSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.filter.SelectorDimFilter;
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
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    final SystemTableDataProvider provider = Mockito.mock(SystemTableDataProvider.class);
    Mockito.when(provider.getPushdownFilters()).thenReturn(List.of(new SystemTablePushdownFilter("server", null)));
    Mockito.when(provider.getRows(Mockito.anyList(), Mockito.any())).thenReturn(
        List.<Object[]>of(new Object[]{BROKER.getHostAndPortToUse(), "broker", "[broker]", "key", "value", null})
    );
    final SelectorDimFilter filter = new SelectorDimFilter("server", BROKER.getHostAndPortToUse(), null);

    consume(
        reader(httpClient, Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider)).attach(
            0,
            slice(List.of(source(BROKER, NodeRole.BROKER)), filter, List.of("server"), Long.MAX_VALUE),
            new CounterTracker(false),
            ignored -> {
            }
        )
    );

    final ArgumentCaptor<List<org.apache.druid.query.filter.DimFilter>> filters = ArgumentCaptor.forClass(List.class);
    Mockito.verify(provider).getRows(filters.capture(), Mockito.any());
    Assertions.assertEquals(List.of(filter), filters.getValue());
    Mockito.verifyNoInteractions(httpClient);
  }

  /** Remote server properties are read from the existing status endpoint, not from the native-query endpoint. */
  @Test
  public void testAttachReadsRemoteStatusProperties()
  {
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    Mockito.when(httpClient.go(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Futures.immediateFuture(response(HttpResponseStatus.OK, "{\"key\":\"value\"}")));

    consume(
        reader(httpClient, Map.of()).attach(
            0,
            slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
            new CounterTracker(false),
            ignored -> {
            }
        )
    );

    final ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(httpClient).go(requestCaptor.capture(), Mockito.any(), Mockito.any());
    Assertions.assertEquals("/status/properties", requestCaptor.getValue().getUrl().getPath());
  }

  /** All remote status requests start before the reader waits for the first response. */
  @Test
  public void testAttachStartsRemoteRequestsTogether()
  {
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    final SettableFuture<StringFullResponseHolder> firstResponse = SettableFuture.create();
    final AtomicInteger requestsStarted = new AtomicInteger();
    Mockito.when(httpClient.go(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
      if (requestsStarted.incrementAndGet() == 1) {
        return firstResponse;
      }
      firstResponse.set(response(HttpResponseStatus.OK, "{\"first\":\"value\"}"));
      return Futures.immediateFuture(response(HttpResponseStatus.OK, "{\"second\":\"value\"}"));
    });

    consume(
        reader(httpClient, Map.of()).attach(
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
        )
    );

    Assertions.assertEquals(2, requestsStarted.get());
  }

  /** Authorization and other non-availability HTTP failures fail the query rather than becoming data rows. */
  @Test
  public void testAttachPropagatesHttpFailure()
  {
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    Mockito.when(httpClient.go(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Futures.immediateFuture(response(HttpResponseStatus.FORBIDDEN, "forbidden")));
    final PhysicalInputSlice inputSlice = reader(httpClient, Map.of()).attach(
        0,
        slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertThrows(RuntimeException.class, () -> consume(inputSlice));
  }

  /** A node transport failure is represented by the descriptor's availability row. */
  @Test
  public void testAttachRecoversNodeAvailabilityFailure()
  {
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    Mockito.when(httpClient.go(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Futures.immediateFailedFuture(new SocketException("connection refused")));

    Assertions.assertDoesNotThrow(
        () -> consume(
            reader(httpClient, Map.of()).attach(
                0,
                slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
                new CounterTracker(false),
                ignored -> {
                }
            )
        )
    );
  }

  /** A remote HTTP read timeout retains query-timeout semantics rather than becoming an availability row. */
  @Test
  public void testAttachPropagatesRemoteTimeoutAsQueryTimeout()
  {
    final HttpClient httpClient = Mockito.mock(HttpClient.class);
    Mockito.when(httpClient.go(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Futures.immediateFailedFuture(new ReadTimeoutException()));
    final PhysicalInputSlice inputSlice = reader(httpClient, Map.of()).attach(
        0,
        slice(List.of(source(HISTORICAL_ONE, NodeRole.HISTORICAL)), null, null, Long.MAX_VALUE),
        new CounterTracker(false),
        ignored -> {
        }
    );

    Assertions.assertThrows(QueryTimeoutException.class, () -> consume(inputSlice));
  }

  /** A request added concurrently after slice cleanup is cancelled immediately. */
  @Test
  public void testRemoteRequestTrackerCancelsRequestAddedAfterClose()
  {
    final DartSystemTableInputSliceReader.RemoteRequestTracker tracker =
        new DartSystemTableInputSliceReader.RemoteRequestTracker();
    final SettableFuture<StringFullResponseHolder> request = SettableFuture.create();

    tracker.cancelAll();
    tracker.track(request);

    Assertions.assertTrue(request.isCancelled());
  }

  private static DartSystemTableInputSliceReader reader(
      final HttpClient httpClient,
      final Map<String, SystemTableDataProvider> providers
  )
  {
    return new DartSystemTableInputSliceReader(
        httpClient,
        new DefaultObjectMapper(),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, DESCRIPTOR),
        providers,
        BROKER,
        Mockito.mock(AuthenticationResult.class),
        Mockito.mock(AuthorizerMapper.class),
        QueryContext.empty()
    );
  }

  private static StringFullResponseHolder response(final HttpResponseStatus status, final String content)
  {
    return new StringFullResponseHolder(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, status),
        StandardCharsets.UTF_8
    ).addChunk(content);
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
    final ServerPropertiesTableDescriptor delegate = new ServerPropertiesTableDescriptor();
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getRowSignature()).thenReturn(ServerPropertiesTableDescriptor.ROW_SIGNATURE);
    Mockito.when(descriptor.getRowAuthorizer()).thenReturn((rows, authenticationResult, authorizerMapper) -> rows);
    Mockito.when(descriptor.getNodeFailureRow(Mockito.any(), Mockito.anySet(), Mockito.any())).thenAnswer(
        invocation -> delegate.getNodeFailureRow(
            invocation.getArgument(0),
            invocation.getArgument(1),
            invocation.getArgument(2)
        )
    );
    return descriptor;
  }

  private static void consume(final PhysicalInputSlice physicalInputSlice)
  {
    final AcquireSegmentAction action = physicalInputSlice.getLoadableSegments().get(0).acquire(AcquireMode.FULL);
    try (action) {
      final AcquireSegmentResult acquireResult = FutureUtils.getUnchecked(action.getSegmentFuture(), false);
      final Optional<Segment> segmentReference = acquireResult.getReferenceProvider().acquireReference();
      Assertions.assertTrue(segmentReference.isPresent());
      try (Segment segment = segmentReference.orElseThrow();
           CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        final Cursor cursor = holder.asCursor();
        while (cursor != null && !cursor.isDone()) {
          cursor.advance();
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

  private static DruidNode node(final String service, final int port)
  {
    return new DruidNode(service, "localhost", false, port, -1, true, false);
  }
}
