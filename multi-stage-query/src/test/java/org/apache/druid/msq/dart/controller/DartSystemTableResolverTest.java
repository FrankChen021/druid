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

import com.google.inject.Injector;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.system.query.SystemTableQueryClient;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public class DartSystemTableResolverTest
{
  private static final String TABLE = "server_properties";
  private static final RowSignature SIGNATURE = RowSignature.builder().add("server", ColumnType.STRING).build();
  private static final AuthenticationResult CALLER = new AuthenticationResult("caller", "basic", null, null);

  private final AuthorizerMapper authorizerMapper = Mockito.mock(AuthorizerMapper.class);
  private final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
  private final SystemTableQueryClient queryClient = Mockito.mock(SystemTableQueryClient.class);
  private final Injector injector = Mockito.mock(Injector.class);
  private DartSystemTableResolver resolver;

  @BeforeEach
  public void setUp()
  {
    Mockito.when(injector.getInstance(SystemTableQueryClient.class)).thenReturn(queryClient);
    resolver = new DartSystemTableResolver(Map.of(TABLE, descriptor), authorizerMapper, injector);
  }

  /**
   * The table is gathered from every service with the caller's access, and the query reads the gathered rows.
   */
  @Test
  public void testResolveGathersRowsWithCallersAccess() throws Exception
  {
    final SystemTableAccess access = new SystemTableAccess(row -> true);
    Mockito.when(descriptor.getRowAuthorizer()).thenReturn((authenticationResult, mapper) -> access);
    final InlineDataSource rows = InlineDataSource.fromIterable(List.<Object[]>of(new Object[]{"broker"}), SIGNATURE);
    final Closeable closer = Mockito.mock(Closeable.class);
    final ArgumentCaptor<Query<Object>> sentQuery = ArgumentCaptor.captor();
    Mockito.when(queryClient.resolve(sentQuery.capture()))
           .thenAnswer(invocation -> new SystemTableQueryClient.ResolvedQuery<>(
               invocation.<Query<Object>>getArgument(0).withDataSource(rows),
               closer
           ));

    final Query<?> resolved = resolver.resolve(scan(new SystemTableDataSource(TABLE)), CALLER);

    Assertions.assertEquals(rows, resolved.getDataSource());
    final SystemTableDataSource sentTable = (SystemTableDataSource) sentQuery.getValue().getDataSource();
    Assertions.assertEquals(SystemTableDataSource.Scope.CLUSTER, sentTable.getScope());
    Assertions.assertSame(access, sentTable.getAccess());
    Mockito.verify(closer).close();
  }

  /**
   * A caller who may not read the table is rejected before any service is asked for rows.
   */
  @Test
  public void testResolveRejectsForbiddenCaller()
  {
    Mockito.when(descriptor.getRowAuthorizer()).thenReturn((authenticationResult, mapper) -> {
      throw new ForbiddenException("no STATE READ");
    });

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> resolver.resolve(scan(new QueryDataSource(scan(new SystemTableDataSource(TABLE)))), CALLER)
    );
    Mockito.verifyNoInteractions(queryClient);
  }

  /**
   * The embedded Broker worker can't read segments, so system tables can't be mixed with other tables.
   */
  @Test
  public void testResolveRejectsMixedDataSources()
  {
    Mockito.when(descriptor.getRowAuthorizer()).thenReturn((authenticationResult, mapper) -> SystemTableAccess.ALL_ROWS);
    final DataSource mixed = new UnionDataSource(
        List.of(new SystemTableDataSource(TABLE), new TableDataSource("foo"))
    );

    final DruidException e = Assertions.assertThrows(DruidException.class, () -> resolver.resolve(scan(mixed), CALLER));
    Assertions.assertEquals("Dart queries can't read system tables together with other datasources", e.getMessage());
    Mockito.verifyNoInteractions(queryClient);
  }

  @Test
  public void testSupportsOnlyRegisteredTables()
  {
    Assertions.assertTrue(resolver.supports(TABLE));
    Assertions.assertFalse(resolver.supports("unknown"));
  }

  private static ScanQuery scan(final DataSource dataSource)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(dataSource)
                 .eternityInterval()
                 .columns("server")
                 .build();
  }
}
