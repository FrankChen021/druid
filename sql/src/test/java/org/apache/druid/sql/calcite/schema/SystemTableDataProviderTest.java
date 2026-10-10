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

package org.apache.druid.sql.calcite.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class SystemTableDataProviderTest
{
  @Test
  public void testSystemSchemaSelectsNativeTablesOnlyForCapableEngine()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    final PlannerContext incapableContext = EasyMock.createMock(PlannerContext.class);
    final PlannerContext enabledContext = EasyMock.createMock(PlannerContext.class);
    final SqlEngine incapableEngine = EasyMock.createMock(SqlEngine.class);
    final SqlEngine enabledEngine = EasyMock.createMock(SqlEngine.class);
    EasyMock.expect(table.getQualifiedName()).andStubReturn(List.of("sys", "server_properties"));
    EasyMock.expect(table.unwrap(SystemTableDataSourceTable.class)).andReturn(ServerPropertiesDataSourceTable::new).times(2);
    EasyMock.expect(incapableContext.getEngine()).andReturn(incapableEngine).once();
    EasyMock.expect(incapableEngine.supportsSystemTableDataSource("server_properties")).andReturn(false).once();
    EasyMock.expect(enabledContext.getEngine()).andReturn(enabledEngine).once();
    EasyMock.expect(enabledEngine.supportsSystemTableDataSource("server_properties")).andReturn(true).once();
    EasyMock.replay(
        table,
        incapableContext,
        enabledContext,
        incapableEngine,
        enabledEngine
    );

    Assertions.assertFalse(SystemSchema.canUseSystemTableDataSource(table, incapableContext));
    Assertions.assertTrue(SystemSchema.canUseSystemTableDataSource(table, enabledContext));

    EasyMock.verify(
        table,
        incapableContext,
        enabledContext,
        incapableEngine,
        enabledEngine
    );
  }

  @ParameterizedTest
  @MethodSource("systemTableRepresentations")
  public void testSystemSchemaGetsOnlyRegisteredRepresentation(
      final List<String> qualifiedName,
      final SystemTableDataSourceTable capability,
      final boolean expectedPresent
  )
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.getQualifiedName()).andStubReturn(qualifiedName);
    EasyMock.expect(table.unwrap(SystemTableDataSourceTable.class)).andReturn(capability).once();
    EasyMock.replay(table);

    final DruidTable nativeTable = SystemSchema.getSystemTableDataSourceTable(table);
    if (expectedPresent) {
      Assertions.assertInstanceOf(ServerPropertiesDataSourceTable.class, nativeTable);
    } else {
      Assertions.assertNull(nativeTable);
    }
    EasyMock.verify(table);
  }

  private static Stream<Arguments> systemTableRepresentations()
  {
    return Stream.of(
        Arguments.of(
            List.of("sys", "server_properties"),
            (SystemTableDataSourceTable) ServerPropertiesDataSourceTable::new,
            true
        ),
        Arguments.of(List.of("sys", "tasks"), null, false)
    );
  }
}
