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


package org.apache.druid.server.system.handler;

import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SystemTableQueryHandlerTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test-user", AuthConfig.ALLOW_ALL_NAME, null, null);

  /** A node-local Scan with LIMIT/OFFSET returns the rows after the offset, as a Scan over any other datasource does. */
  @Test
  public void testScanOffsetAndLimit()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(ServerPropertiesTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .columns("property")
                                  .offset(1)
                                  .limit(2)
                                  .build();

    Assertions.assertEquals(List.of(List.of("b"), List.of("c")), run(query, "a", "b", "c", "d"));
  }

  /** Without an offset the same Scan returns the leading rows, which pins the expectation above to the offset only. */
  @Test
  public void testScanLimit()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource(ServerPropertiesTableDescriptor.TABLE_NAME))
                                  .eternityInterval()
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .columns("property")
                                  .limit(2)
                                  .build();

    Assertions.assertEquals(List.of(List.of("a"), List.of("b")), run(query, "a", "b", "c", "d"));
  }

  private static List<List<Object>> run(final ScanQuery query, final String... properties)
  {
    final List<Object[]> rows = new ArrayList<>();
    for (final String property : properties) {
      rows.add(new Object[]{"localhost:8080", "overlord", "[overlord]", property, "value", null});
    }
    final SystemTableDataProvider provider = Mockito.mock(SystemTableDataProvider.class);
    Mockito.when(provider.getPushdownFilters()).thenReturn(List.of());
    Mockito.when(provider.getRows(Mockito.anyList(), Mockito.any())).thenReturn(rows);
    final AuthorizerMapper allowAll = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return (authenticationResult, resource, action) -> Access.OK;
      }
    };
    final SystemTableQueryHandler handler = new SystemTableQueryHandler(
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, provider),
        Map.of(ServerPropertiesTableDescriptor.TABLE_NAME, new ServerPropertiesTableDescriptor()),
        new ScanQueryEngine(),
        allowAll
    );

    final QueryRunner<ScanResultValue> runner = handler.createRunner(query, AUTHENTICATION_RESULT, true);
    final List<List<Object>> result = new ArrayList<>();
    for (final ScanResultValue value : runner.run(QueryPlus.wrap(query), ResponseContext.createEmpty()).toList()) {
      for (final Object event : (List<?>) value.getEvents()) {
        result.add(event instanceof Object[] array ? List.of(array) : new ArrayList<>((List<?>) event));
      }
    }
    return result;
  }
}
