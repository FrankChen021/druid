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

package org.apache.druid.msq.input.system;

import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class SystemTableInputSpecTest
{
  /** A single system-table leaf receives the native filter, required-column projection, and safe source limit. */
  @Test
  public void testAddSourceHintsForSingleSystemTable()
  {
    final Query<?> query = Mockito.mock(Query.class);
    final SelectorDimFilter filter = new SelectorDimFilter("server", "historical:8083", null);
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("v0", "concat(\"server\", 'x')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    Mockito.when(query.getFilter()).thenReturn(filter);
    Mockito.when(query.getRequiredColumns()).thenReturn(Set.of("value", "server"));
    Mockito.when(query.getVirtualColumns()).thenReturn(virtualColumns);

    Assertions.assertEquals(
        List.of(new SystemTableInputSpec("server_properties", filter, List.of("server", "value"), virtualColumns, 11)),
        SystemTableInputSpec.addSourceHints(
            List.of(new SystemTableInputSpec("server_properties")),
            query,
            11
        )
    );
  }

  /** An explicitly empty Scan projection remains empty so the worker can use its cardinality-only transport path. */
  @Test
  public void testAddSourceHintsPreservesEmptyScanProjection()
  {
    final ScanQuery query = Mockito.mock(ScanQuery.class);
    Mockito.when(query.getColumns()).thenReturn(List.of());
    Mockito.when(query.getVirtualColumns()).thenReturn(VirtualColumns.EMPTY);

    Assertions.assertEquals(
        List.of(new SystemTableInputSpec("server_properties", null, List.of(), VirtualColumns.EMPTY, Long.MAX_VALUE)),
        SystemTableInputSpec.addSourceHints(
            List.of(new SystemTableInputSpec("server_properties")),
            query,
            Long.MAX_VALUE
        )
    );
    Mockito.verify(query, Mockito.never()).getRequiredColumns();
  }

  /** An empty Scan projection with a residual filter keeps the full input because Scan cannot report its dependencies. */
  @Test
  public void testAddSourceHintsDoesNotPruneEmptyFilteredScan()
  {
    final ScanQuery query = Mockito.mock(ScanQuery.class);
    final SelectorDimFilter filter = new SelectorDimFilter("property", "foo", null);
    Mockito.when(query.getColumns()).thenReturn(List.of());
    Mockito.when(query.getFilter()).thenReturn(filter);
    Mockito.when(query.getVirtualColumns()).thenReturn(VirtualColumns.EMPTY);
    Mockito.when(query.getRequiredColumns()).thenReturn(null);

    Assertions.assertEquals(
        List.of(new SystemTableInputSpec("server_properties", filter, null, VirtualColumns.EMPTY, Long.MAX_VALUE)),
        SystemTableInputSpec.addSourceHints(
            List.of(new SystemTableInputSpec("server_properties")),
            query,
            Long.MAX_VALUE
        )
    );
  }

  /** Root predicates are not assigned to a leaf when a stage contains more than one system-table input. */
  @Test
  public void testAddSourceHintsLeavesCompositeInputUnchanged()
  {
    final Query<?> query = Mockito.mock(Query.class);
    final List<InputSpec> inputSpecs = List.of(
        new SystemTableInputSpec("server_properties"),
        new SystemTableInputSpec("server_properties")
    );

    Assertions.assertSame(inputSpecs, SystemTableInputSpec.addSourceHints(inputSpecs, query, 11));
    Mockito.verifyNoInteractions(query);
  }
}
