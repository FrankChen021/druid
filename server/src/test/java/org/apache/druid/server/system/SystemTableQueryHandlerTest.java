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

import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
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
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.handler.SystemTableQueryHandler;
import org.apache.druid.server.system.table.ServerPropertiesTableDataProvider;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRowAuthorizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class SystemTableQueryHandlerTest
{
  private static final String INTERNAL_IDENTITY = "druid_system";
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                               .add("task_id", ColumnType.STRING)
                                                               .add("duration", ColumnType.LONG)
                                                               .build();
  private static final RowSignature TRANSPORT_ROW_SIGNATURE = RowSignature.builder()
                                                                         .addAll(ROW_SIGNATURE)
                                                                         .add("internal", ColumnType.STRING)
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
      public Iterable<Object[]> getRows(final List<DimFilter> filters)
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
        return (rows, authenticationResult, _) -> {
          authorizationCalls.incrementAndGet();
          Assertions.assertEquals("alice", authenticationResult.getIdentity());
          return () -> StreamSupport.stream(rows.spliterator(), false)
                                    .filter(row -> !"task-a".equals(row[0]))
                                    .iterator();
        };
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of("test", supplier),
        Map.of(descriptor.getTableName(), descriptor),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of()),
        NoopEscalator.getInstance()
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

  /**
   * A virtual column shadows the physical column of the same name. A filter on the virtual {@code server} column must
   * be evaluated by the native scan rather than pushed to the provider, which compares it to the physical host.
   */
  @Test
  public void testFilterOnVirtualColumnShadowingPushdownColumnKeepsMatchingRows()
  {
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) -> Access.OK;
      }
    };
    final Properties properties = new Properties();
    properties.setProperty("druid.test.visible", "visible");
    final ServerPropertiesTableDataProvider provider = new ServerPropertiesTableDataProvider(
        new DruidNode("overlord", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.OVERLORD),
        properties,
        new DruidServerConfig(null, null)
    );
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, new ServerPropertiesTableDescriptor()),
        new ScanQueryEngine(),
        authorizerMapper,
        NoopEscalator.getInstance()
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(ServerPropertiesTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .virtualColumns(
                                      new ExpressionVirtualColumn(
                                          "server",
                                          "'alias'",
                                          ColumnType.STRING,
                                          ExprMacroTable.nil()
                                      )
                                  )
                                  .filters(new SelectorDimFilter("server", "alias", null))
                                  .columns("property", "value")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();

    final List<ScanResultValue> result = handler.createRunner(
        query,
        new AuthenticationResult("alice", AuthConfig.ALLOW_ALL_NAME, null, null),
        true
    ).run(QueryPlus.wrap(query), ResponseContext.createEmpty()).toList();

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(List.of(List.of("druid.test.visible", "visible")), result.get(0).getEvents());
  }

  /**
   * Node-local execution applies the table's row authorizer to the request identity. For {@code server_properties}
   * that requires STATE READ, which the provider itself no longer checks.
   */
  @Test
  public void testServerPropertiesRejectsCallerWithoutStateRead()
  {
    final AuthorizerMapper denyState = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) ->
            ResourceType.STATE.equals(resource.getType()) ? Access.DENIED : Access.OK;
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of(
            ServerPropertiesTableDescriptor.TABLE_NAME,
            new ServerPropertiesTableDataProvider(
                new DruidNode("overlord", "localhost", false, 8080, null, true, false),
                Set.of(NodeRole.OVERLORD),
                new Properties(),
                new DruidServerConfig(null, null)
            )
        ),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, new ServerPropertiesTableDescriptor()),
        new ScanQueryEngine(),
        denyState,
        NoopEscalator.getInstance()
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(ServerPropertiesTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .build();

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> handler.createRunner(query, new AuthenticationResult("alice", "deny", null, null), true)
                     .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                     .toList()
    );
  }

  /** A node-local handler rejects a composite root instead of casting it to a system-table datasource. */
  @Test
  public void testRejectsCompositeDataSource()
  {
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of(),
        Map.of(),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of()),
        NoopEscalator.getInstance()
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(
                                      FilteredDataSource.create(
                                          new SystemTableDataSource("server_properties"),
                                          new SelectorDimFilter("property", "druid.host", null)
                                      )
                                  )
                                  .eternityInterval()
                                  .build();

    Assertions.assertThrows(
        DruidException.class,
        () -> handler.createRunner(
            query,
            new AuthenticationResult("alice", "allow", "external", null),
            true
        )
    );
  }

  /**
   * The Broker's fanout runs as the escalated identity and needs the transport columns to re-authorize rows against
   * the caller. A user's own request with the local native query route runs as the user and gets the public table.
   */
  @Test
  public void testTransportColumnsAreReturnedOnlyToInternalLocalRequests()
  {
    final SystemTableDataProvider supplier = filters -> List.<Object[]>of(
        new Object[]{"task-a", 10L, "authorization-metadata"}
    );
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
      public RowSignature getTransportRowSignature()
      {
        return TRANSPORT_ROW_SIGNATURE;
      }

      @Override
      public Object[] toPublicRow(final Object[] transportRow)
      {
        return Arrays.copyOf(transportRow, ROW_SIGNATURE.size());
      }

      @Override
      public SystemTableRowAuthorizer getRowAuthorizer()
      {
        return (rows, authenticationResult, authorizerMapper) -> rows;
      }
    };
    final Escalator escalator = Mockito.mock(Escalator.class);
    Mockito.when(escalator.createEscalatedAuthenticationResult())
           .thenReturn(new AuthenticationResult(INTERNAL_IDENTITY, "allow", null, null));
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of("test", supplier),
        Map.of(descriptor.getTableName(), descriptor),
        new ScanQueryEngine(),
        new AuthorizerMapper(Map.of()),
        escalator
    );

    // A node that authenticates the escalated client sets its own authenticatedBy, so only the identity matches.
    final AuthenticationResult internal = new AuthenticationResult(INTERNAL_IDENTITY, "allow", "basic", null);
    final AuthenticationResult user = new AuthenticationResult("alice", "allow", "external", null);

    Assertions.assertEquals(
        Arrays.asList("task-a", 10L, "authorization-metadata"),
        runScan(handler, TRANSPORT_ROW_SIGNATURE, internal, true)
    );
    Assertions.assertEquals(
        Arrays.asList("task-a", 10L, null),
        runScan(handler, TRANSPORT_ROW_SIGNATURE, user, true)
    );
    Assertions.assertEquals(
        Arrays.asList("task-a", 10L, null),
        runScan(handler, TRANSPORT_ROW_SIGNATURE, internal, false)
    );
  }

  private static List<?> runScan(
      final SystemTableQueryHandler handler,
      final RowSignature columns,
      final AuthenticationResult authenticationResult,
      final boolean executeLocally
  )
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("test"))
                                  .eternityInterval()
                                  .columns(columns)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build();
    final QueryRunner<ScanResultValue> runner = handler.createRunner(
        query,
        authenticationResult,
        executeLocally
    );
    final List<ScanResultValue> result = runner.run(
        QueryPlus.wrap(query),
        ResponseContext.createEmpty()
    ).toList();

    Assertions.assertEquals(1, result.size());
    return (List<?>) ((List<?>) result.get(0).getEvents()).get(0);
  }
}
