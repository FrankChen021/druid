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

package org.apache.druid.msq.querykit.datasource;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;

import java.util.List;

/** Plans the system-table datasource supported by the initial Dart integration. */
public class SystemTableDataSourcePlanner implements DataSourcePlanner<SystemTableDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final SystemTableDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    DataSourcePlannerUtils.checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
    if (!ServerPropertiesTableDescriptor.TABLE_NAME.equals(dataSource.getTable())) {
      throw InvalidInput.exception("Dart system-table execution does not support table[sys.%s]", dataSource.getTable());
    }

    return new DataSourcePlan(
        new InputNumberDataSource(0),
        List.of(new SystemTableInputSpec(dataSource.getTable())),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }
}
