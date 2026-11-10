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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.system.module.SystemTableModule;
import org.apache.druid.server.system.table.StackTraceTableDescriptor;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

/**
 * Metadata representation of the native-query-only {@code sys.stack_trace} table.
 * <p>
 * The table is served only by the Druid engine, through {@link #asDataSourceTable()}. When the Druid engine can't
 * serve it, {@link #toRel} fails while the query is converted to a plan, wherever the table appears in the query.
 * <p>
 * The table also implements {@link ScannableTable} for queries that the Druid engine could serve but that also read
 * system tables only the Broker's SQL interpreter (Bindable) can serve, which sends the whole query to the
 * interpreter. Its {@link #scan} then fails with the same error.
 */
public class SystemStackTraceTable extends AbstractTable
    implements SystemTableDataSourceTable, ScannableTable, TranslatableTable
{
  private final boolean enableDruidEngineForSystemTables;

  public SystemStackTraceTable(final boolean enableDruidEngineForSystemTables)
  {
    this.enableDruidEngineForSystemTables = enableDruidEngineForSystemTables;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(StackTraceTableDescriptor.ROW_SIGNATURE, typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Nullable
  @Override
  public StackTraceDataSourceTable asDataSourceTable()
  {
    return enableDruidEngineForSystemTables ? new StackTraceDataSourceTable() : null;
  }

  /**
   * Fails unless the Druid engine can serve this table, which {@link SystemSchema#canUseSystemTableDataSource} decides
   * from the {@link PlannerContext} of the query.
   */
  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    final PlannerContext plannerContext = context.getCluster().getPlanner().getContext().unwrap(PlannerContext.class);
    if (plannerContext == null || !SystemSchema.canUseSystemTableDataSource(table, plannerContext)) {
      throw druidEngineRequiredException();
    }
    return LogicalTableScan.create(context.getCluster(), table, context.getTableHints());
  }

  /**
   * Always fails: the Broker's SQL interpreter can't serve this table.
   */
  @Override
  public Enumerable<Object[]> scan(final DataContext root)
  {
    throw druidEngineRequiredException();
  }

  /**
   * Returns the error for a query on this table that the Druid engine can't serve.
   */
  public static DruidException druidEngineRequiredException()
  {
    return InvalidInput.exception(
        "%s.%s can only be queried with the Druid engine: set the query context parameter %s to true and use the "
        + "native SQL engine. The Druid engine must also be enabled with %s on all services.",
        NamedSystemSchema.NAME,
        StackTraceTableDescriptor.TABLE_NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES.getName(),
        SystemTableModule.ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY
    );
  }
}
