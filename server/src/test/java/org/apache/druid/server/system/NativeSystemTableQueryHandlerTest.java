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

import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.NoopEscalator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NativeSystemTableQueryHandlerTest
{
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                               .add("task_id", ColumnType.STRING)
                                                               .add("duration", ColumnType.LONG)
                                                               .build();

  @Test
  public void testRunsComponentScanAgainstSuppliedRows()
  {
    final NativeSystemTableDataSupplier supplier = new NativeSystemTableDataSupplier()
    {
      @Override
      public RowSignature getRowSignature()
      {
        return ROW_SIGNATURE;
      }

      @Override
      public Iterable<Object[]> getRows(
          final List<DimFilter> extractedFilters,
          final AuthenticationResult authenticationResult
      )
      {
        return Arrays.asList(
            new Object[]{"task-a", 10L},
            new Object[]{"task-b", 20L}
        );
      }
    };

    final NativeSystemTableQueryHandler handler = new NativeSystemTableQueryHandler(
        Map.of("test", supplier),
        new ScanQueryEngine(),
        new ServiceEmitter("test", "localhost", new NoopEmitter()),
        NoopEscalator.getInstance()
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("test"))
                                  .eternityInterval()
                                  .columns(ROW_SIGNATURE)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .context(
                                      Map.of(
                                          SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_COMPONENT_LOCAL,
                                          true
                                      )
                                  )
                                  .build();

    final QueryRunner<ScanResultValue> runner = handler.createRunner(
        query,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );

    final List<ScanResultValue> result = runner.run(
        QueryPlus.wrap(query),
        ResponseContext.createEmpty()
    ).toList();

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(
        List.of(
            List.of("task-a", 10L),
            List.of("task-b", 20L)
        ),
        result.get(0).getEvents()
    );
  }
}
