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

package org.apache.druid.server.system;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.handler.SystemTableQueryHandler;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRowAuthorizer;
import org.apache.druid.server.system.table.SystemTableQueryRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SystemTableQueryHandlerTest
{
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                               .add("task_id", ColumnType.STRING)
                                                               .add("duration", ColumnType.LONG)
                                                               .build();

  /** A direct local Scan lazily reads provider rows and applies the table descriptor's row authorization. */
  @Test
  public void testRunsNodeScanAgainstSuppliedRows()
  {
    final AtomicInteger getRowsCalls = new AtomicInteger();
    final AtomicInteger authorizationCalls = new AtomicInteger();
    final SystemTableDataProvider supplier = new SystemTableDataProvider()
    {
      @Override
      public Iterable<Object[]> getRows(
          final List<DimFilter> filters,
          final AuthenticationResult authenticationResult
      )
      {
        getRowsCalls.incrementAndGet();
        return Arrays.asList(
            new Object[]{"task-a", 10L},
            new Object[]{"task-b", 20L},
            new Object[]{"task-c", 30L}
        );
      }
    };

    final SystemTableDescriptor descriptor = new SystemTableDescriptor()
    {
      @Override
      public String getTableName()
      {
        return "test";
      }

      @Override
      public Set<NodeRole> getNodeRoles()
      {
        return Set.of();
      }

      @Override
      public RowSignature getRowSignature()
      {
        return ROW_SIGNATURE;
      }

      @Override
      public SystemTableRowAuthorizer getRowAuthorizer()
      {
        return (rows, authenticationResult, authorizerMapper) -> {
          authorizationCalls.incrementAndGet();
          Assertions.assertEquals("alice", authenticationResult.getIdentity());
          return () -> java.util.stream.StreamSupport.stream(rows.spliterator(), false)
                                                      .filter(row -> !"task-a".equals(row[0]))
                                                      .iterator();
        };
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of("test", supplier),
        Map.of(descriptor.getTableName(), descriptor),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of())
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("test"))
                                  .eternityInterval()
                                  .columns(ROW_SIGNATURE)
                                  .filters(new SelectorDimFilter("task_id", "task-b", null))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final QueryRunner<ScanResultValue> runner = handler.createRunner(
        query,
        new AuthenticationResult("alice", "allow", "external", null),
        true
    );
    Assertions.assertEquals(0, getRowsCalls.get());

    final List<ScanResultValue> result = runner.run(
        QueryPlus.wrap(query),
        ResponseContext.createEmpty()
    ).toList();

    Assertions.assertEquals(1, getRowsCalls.get());
    Assertions.assertEquals(1, authorizationCalls.get());
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(
        List.of(
            List.of("task-b", 20L)
        ),
        result.get(0).getEvents()
    );
  }

  @Test
  public void testDirectResolutionAuthorizesRawRowsBeforeProjection()
  {
    final AtomicInteger getRawRowsCalls = new AtomicInteger();
    final AtomicInteger projectRowCalls = new AtomicInteger();
    final AtomicInteger authorizationCalls = new AtomicInteger();
    final SystemTableDataProvider supplier = new SystemTableDataProvider()
    {
      @Override
      public Iterable<Object[]> getRows(
          final List<DimFilter> filters,
          final AuthenticationResult authenticationResult
      )
      {
        throw new AssertionError("Direct resolution must use raw rows");
      }

      @Override
      public Iterable<Object[]> getRawRows(
          final List<DimFilter> filters,
          final AuthenticationResult authenticationResult
      )
      {
        getRawRowsCalls.incrementAndGet();
        return List.of(
            new Object[]{"task-a", 10L},
            new Object[]{"task-b", 20L}
        );
      }

      @Override
      public Object[] projectRow(final Object[] row, final int[] projects)
      {
        projectRowCalls.incrementAndGet();
        return SystemTableDataProvider.super.projectRow(row, projects);
      }
    };
    final SystemTableDescriptor descriptor = new SystemTableDescriptor()
    {
      @Override
      public String getTableName()
      {
        return "test";
      }

      @Override
      public Set<NodeRole> getNodeRoles()
      {
        return Set.of();
      }

      @Override
      public RowSignature getRowSignature()
      {
        return ROW_SIGNATURE;
      }

      @Override
      public SystemTableRowAuthorizer getRowAuthorizer()
      {
        return (rows, authenticationResult, authorizerMapper) -> {
          authorizationCalls.incrementAndGet();
          return () -> java.util.stream.StreamSupport.stream(rows.spliterator(), false)
                                                    .filter(row -> "task-b".equals(row[0]))
                                                    .iterator();
        };
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of("test", supplier),
        Map.of("test", descriptor),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of())
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("test"))
                                  .eternityInterval()
                                  .columns(List.of("duration"))
                                  .build();

    final InlineDataSource inlineDataSource = Assertions.assertInstanceOf(
        InlineDataSource.class,
        handler.resolveDataSource(
            query,
            new AuthenticationResult("alice", "allow", "external", null)
        )
    );

    Assertions.assertEquals(List.of("duration"), inlineDataSource.getRowSignature().getColumnNames());
    Assertions.assertEquals(0, getRawRowsCalls.get());
    Assertions.assertEquals(0, projectRowCalls.get());
    Assertions.assertEquals(1, authorizationCalls.get());
    final List<Object[]> rows = inlineDataSource.getRowsAsList();
    Assertions.assertEquals(1, getRawRowsCalls.get());
    Assertions.assertEquals(1, projectRowCalls.get());
    Assertions.assertArrayEquals(new Object[]{20L}, rows.get(0));
  }

  @Test
  public void testProviderDataSourceReceivesOnlyAuthorizedRows()
  {
    final AtomicReference<List<Object[]>> rowsReceivedByProvider = new AtomicReference<>();
    final AtomicReference<SystemTableQueryRequest> requestReceivedByProvider = new AtomicReference<>();
    final SystemTableDataProvider supplier = new SystemTableDataProvider()
    {
      @Override
      public Iterable<Object[]> getRows(
          final List<DimFilter> filters,
          final AuthenticationResult authenticationResult
      )
      {
        return List.of(
            new Object[]{"task-a", 10L},
            new Object[]{"task-b", 20L}
        );
      }

      @Override
      public Optional<DataSource> getAuthorizedDataSource(
          final SystemTableQueryRequest request,
          final Iterable<Object[]> authorizedRows
      )
      {
        final List<Object[]> rows = java.util.stream.StreamSupport.stream(
            authorizedRows.spliterator(),
            false
        ).toList();
        requestReceivedByProvider.set(request);
        rowsReceivedByProvider.set(rows);
        return Optional.of(
            new BatchedInlineDataSource(
                rows.stream().map(row -> new Object[]{row[1]}).toList(),
                request.rowSignature()
            )
        );
      }
    };
    final SystemTableDescriptor descriptor = new SystemTableDescriptor()
    {
      @Override
      public String getTableName()
      {
        return "test";
      }

      @Override
      public Set<NodeRole> getNodeRoles()
      {
        return Set.of();
      }

      @Override
      public RowSignature getRowSignature()
      {
        return ROW_SIGNATURE;
      }

      @Override
      public SystemTableRowAuthorizer getRowAuthorizer()
      {
        return (rows, authenticationResult, authorizerMapper) ->
            () -> java.util.stream.StreamSupport.stream(rows.spliterator(), false)
                                                .filter(row -> "task-b".equals(row[0]))
                                                .iterator();
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of("test", supplier),
        Map.of("test", descriptor),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of())
    );
    final AuthenticationResult authenticationResult = new AuthenticationResult(
        "alice",
        "allow",
        "external",
        null
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("test"))
                                  .eternityInterval()
                                  .columns(List.of("duration"))
                                  .build();

    final BatchedInlineDataSource dataSource = Assertions.assertInstanceOf(
        BatchedInlineDataSource.class,
        handler.resolveDataSource(query, authenticationResult)
    );

    Assertions.assertEquals(List.of("duration"), requestReceivedByProvider.get().columns());
    Assertions.assertEquals(List.of("duration"), dataSource.getRowSignature().getColumnNames());
    Assertions.assertEquals(1, rowsReceivedByProvider.get().size());
    Assertions.assertArrayEquals(new Object[]{"task-b", 20L}, rowsReceivedByProvider.get().get(0));
  }

}
