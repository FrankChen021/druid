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

import org.apache.calcite.schema.Schema;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.system.table.ConfigurationTableDescriptor;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SystemConfigurationTableTest
{
  @Test
  public void testNativeRepresentation()
  {
    final SystemConfigurationTable table = new SystemConfigurationTable();
    final DruidTable nativeTable = table.asNativeTable();
    Assertions.assertEquals(new SystemTableDataSource("configuration"), nativeTable.getDataSource());
    Assertions.assertEquals(ConfigurationTableDescriptor.ROW_SIGNATURE, nativeTable.getRowSignature());
    Assertions.assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
    Assertions.assertEquals(Schema.TableType.SYSTEM_TABLE, nativeTable.getJdbcTableType());
    Assertions.assertFalse(nativeTable.isBroadcast());
    Assertions.assertFalse(nativeTable.isJoinable());
  }

  @Test
  public void testBindableExecutionExplainsRequiredContext()
  {
    final DruidException error = Assertions.assertThrows(
        DruidException.class,
        () -> new SystemConfigurationTable().scan(null)
    );
    Assertions.assertTrue(error.getMessage().contains("useNativeQueryForSystemTables=true"));
  }
}
