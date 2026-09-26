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
import org.apache.calcite.schema.Schema;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SystemTableDataProviderTest
{
  @Test
  public void testNativeTablesExposeSystemMetadata()
  {
    final ServerPropertiesDataSourceTable serverProperties = new ServerPropertiesDataSourceTable();
    Assertions.assertEquals(
        "server_properties",
        ((SystemTableDataSource) serverProperties.getDataSource()).getTable()
    );
    Assertions.assertFalse(serverProperties.isJoinable());
    Assertions.assertFalse(serverProperties.isBroadcast());
    Assertions.assertEquals(Schema.TableType.SYSTEM_TABLE, serverProperties.getJdbcTableType());
  }

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

  @Test
  public void testSystemSchemaGetsNativeRepresentation()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.unwrap(SystemTableDataSourceTable.class)).andReturn(ServerPropertiesDataSourceTable::new).once();
    EasyMock.replay(table);

    Assertions.assertInstanceOf(ServerPropertiesDataSourceTable.class, SystemSchema.getSystemTableDataSourceTable(table));
    EasyMock.verify(table);
  }

  @Test
  public void testSystemSchemaRejectsTableWithoutNativeCapability()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.unwrap(SystemTableDataSourceTable.class)).andReturn(null).once();
    EasyMock.replay(table);

    Assertions.assertNull(SystemSchema.getSystemTableDataSourceTable(table));
    EasyMock.verify(table);
  }

  @Test
  public void testQualifiedNameAloneDoesNotEnableNativeExecution()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.getQualifiedName()).andStubReturn(List.of("sys", "tasks"));
    EasyMock.expect(table.unwrap(SystemTableDataSourceTable.class)).andReturn(null).once();
    EasyMock.replay(table);

    Assertions.assertNull(SystemSchema.getSystemTableDataSourceTable(table));
    EasyMock.verify(table);
  }
}
