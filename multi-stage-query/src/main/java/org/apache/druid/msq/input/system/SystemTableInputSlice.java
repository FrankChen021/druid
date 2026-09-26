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
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** A group of physical nodes that contribute rows for one system-table input. */
@JsonTypeName("systemTable")
public class SystemTableInputSlice implements InputSlice
{
  private final String table;
  private final List<SystemTableSource> sources;
  @Nullable
  private final DimFilter filter;
  @Nullable
  private final List<String> columns;
  private final VirtualColumns virtualColumns;
  private final long limit;

  public SystemTableInputSlice(
      final String table,
      final List<SystemTableSource> sources,
      @Nullable final DimFilter filter,
      @Nullable final List<String> columns,
      final long limit
  )
  {
    this(table, sources, filter, columns, VirtualColumns.EMPTY, limit);
  }

  @JsonCreator
  public SystemTableInputSlice(
      @JsonProperty("table") final String table,
      @JsonProperty("sources") final List<SystemTableSource> sources,
      @JsonProperty("filter") @Nullable final DimFilter filter,
      @JsonProperty("columns") @Nullable final List<String> columns,
      @JsonProperty("virtualColumns") @Nullable final VirtualColumns virtualColumns,
      @JsonProperty("limit") final long limit
  )
  {
    this.table = Preconditions.checkNotNull(table, "table");
    this.sources = List.copyOf(Preconditions.checkNotNull(sources, "sources"));
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

  @JsonProperty
  public List<SystemTableSource> getSources()
  {
    return sources;
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
  public int fileCount()
  {
    return sources.size();
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
    final SystemTableInputSlice that = (SystemTableInputSlice) o;
    return limit == that.limit
           && Objects.equals(table, that.table)
           && Objects.equals(sources, that.sources)
           && Objects.equals(filter, that.filter)
           && Objects.equals(columns, that.columns)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(table, sources, filter, columns, virtualColumns, limit);
  }

  @Override
  public String toString()
  {
    return "SystemTableInputSlice{" +
           "table='" + table + '\'' +
           ", sources=" + sources +
           ", filter=" + filter +
           ", columns=" + columns +
           ", virtualColumns=" + virtualColumns +
           ", limit=" + limit +
           '}';
  }
}
