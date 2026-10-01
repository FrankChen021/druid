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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SystemTableInputSerdeTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                                .registerModules(new MSQIndexingModule().getJacksonModules());

  @Test
  public void testInputSpecSerde() throws Exception
  {
    final SystemTableInputSpec inputSpec = new SystemTableInputSpec(
        "server_properties",
        new SelectorDimFilter("server", "historical:8083", null),
        List.of("server", "value"),
        VirtualColumns.create(
            new ExpressionVirtualColumn(
                "v0",
                "concat(\"server\", 'x')",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        10
    );
    Assertions.assertEquals(inputSpec, mapper.readValue(mapper.writeValueAsBytes(inputSpec), InputSpec.class));
  }

  @Test
  public void testInputSliceSerde() throws Exception
  {
    final SystemTableInputSlice inputSlice = new SystemTableInputSlice(
        "server_properties",
        List.of(
            new SystemTableSource(
                new DruidNode("historical", "localhost", false, 8083, -1, true, false),
                java.util.Set.of(NodeRole.HISTORICAL)
            )
        ),
        null,
        null,
        Long.MAX_VALUE
    );
    Assertions.assertEquals(inputSlice, mapper.readValue(mapper.writeValueAsBytes(inputSlice), InputSlice.class));
  }
}
