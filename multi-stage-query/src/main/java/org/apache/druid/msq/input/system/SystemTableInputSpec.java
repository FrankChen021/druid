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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Controller-side description of a system-table input. */
@JsonTypeName("systemTable")
public class SystemTableInputSpec implements InputSpec
{
  private final String table;
  @Nullable
  private final DimFilter filter;
  @Nullable
  private final List<String> columns;
  private final VirtualColumns virtualColumns;
  private final long limit;

  public SystemTableInputSpec(final String table)
  {
    this(table, null, null, VirtualColumns.EMPTY, Long.MAX_VALUE);
  }

  /**
   * Adds safe source-side hints for the simple, single-system-table case. The stage query keeps the same filter and
   * limit, so workers still apply the complete Druid semantics after any provider-level prefiltering. Composite
   * datasources are deliberately left unchanged because a root filter cannot generally be assigned to one leaf.
   */
  public static List<InputSpec> addSourceHints(
      final List<InputSpec> inputSpecs,
      final Query<?> query,
      final long sourceLimit
  )
  {
    if (inputSpecs.size() != 1 || !(inputSpecs.get(0) instanceof SystemTableInputSpec inputSpec)) {
      return inputSpecs;
    }

    final List<String> columns;
    if (query instanceof ScanQuery scanQuery
        && scanQuery.getColumns() != null
        && scanQuery.getColumns().isEmpty()
        && scanQuery.getFilter() == null
        && scanQuery.getVirtualColumns().isEmpty()
        && scanQuery.getOrderBys().isEmpty()) {
      // ScanQuery.getRequiredColumns() deliberately returns null for an explicit empty projection, since the native
      // Scan engine normally interprets it as "discover all columns". System-table inputs preserve the distinction:
      // an empty list means that the stage needs row cardinality only, and the reader transports one stable column.
      // This is safe only when the query has no filter, virtual column, or ordering dependency.
      columns = List.of();
    } else {
      final Set<String> requiredColumns = query.getRequiredColumns();
      columns = requiredColumns == null ? null : requiredColumns.stream().sorted().toList();
    }
    return List.of(
        new SystemTableInputSpec(
            inputSpec.table,
            query.getFilter(),
            columns,
            query.getVirtualColumns(),
            sourceLimit
        )
    );
  }

  public SystemTableInputSpec(
      final String table,
      @Nullable final DimFilter filter,
      @Nullable final List<String> columns,
      final long limit
  )
  {
    this(table, filter, columns, VirtualColumns.EMPTY, limit);
  }

  @JsonCreator
  public SystemTableInputSpec(
      @JsonProperty("table") final String table,
      @JsonProperty("filter") @Nullable final DimFilter filter,
      @JsonProperty("columns") @Nullable final List<String> columns,
      @JsonProperty("virtualColumns") @Nullable final VirtualColumns virtualColumns,
      @JsonProperty("limit") final long limit
  )
  {
    this.table = Preconditions.checkNotNull(table, "table");
    this.filter = filter;
    this.columns = columns == null ? null : List.copyOf(columns);
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.limit = limit;
  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }

  @Nullable
  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @Nullable
  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public long getLimit()
  {
    return limit;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SystemTableInputSpec that = (SystemTableInputSpec) o;
    return limit == that.limit
           && Objects.equals(table, that.table)
           && Objects.equals(filter, that.filter)
           && Objects.equals(columns, that.columns)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(table, filter, columns, virtualColumns, limit);
  }

  @Override
  public String toString()
  {
    return "SystemTableInputSpec{" +
           "table='" + table + '\'' +
           ", filter=" + filter +
           ", columns=" + columns +
           ", virtualColumns=" + virtualColumns +
           ", limit=" + limit +
           '}';
  }
}
