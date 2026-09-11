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

import org.apache.druid.server.system.table.QueriesTableDataProvider;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.apache.druid.server.system.table.SystemTableQueryInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

public class QueriesTableDataProviderTest
{
  @Test
  public void testMapsRowsFromAllLocalProviders()
  {
    final SystemTableQueryInfoProvider nativeProvider = () -> List.of(
        new SystemTableQueryInfo(
            "native-1",
            "native",
            "RUNNING",
            "{\"queryType\":\"scan\"}",
            "sql-1",
            false,
            "historical:8083",
            "historical"
        )
    );
    final SystemTableQueryInfoProvider dartProvider = () -> List.of(
        new SystemTableQueryInfo(
            "dart-1",
            "msq-dart",
            "RUNNING",
            "{}",
            "sql-1",
            true,
            "broker:8082",
            "broker"
        )
    );
    final QueriesTableDataProvider provider = new QueriesTableDataProvider(Set.of(nativeProvider, dartProvider));

    final List<Object[]> rows = StreamSupport.stream(provider.getRows(List.of(), null).spliterator(), false).toList();

    Assertions.assertEquals(2, rows.size());
    Assertions.assertTrue(
        rows.stream().anyMatch(
            row -> "native-1".equals(row[0])
                   && "sql-1".equals(row[4])
                   && Long.valueOf(0).equals(row[5])
                   && "historical:8083".equals(row[6])
                   && "historical".equals(row[7])
        )
    );
    Assertions.assertTrue(
        rows.stream().anyMatch(
            row -> "dart-1".equals(row[0])
                   && "sql-1".equals(row[4])
                   && Long.valueOf(1).equals(row[5])
                   && "broker:8082".equals(row[6])
                   && "broker".equals(row[7])
        )
    );
  }
}
