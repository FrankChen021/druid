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

/**
 * A group of physical nodes that contribute rows for one system-table input.
 */
@JsonTypeName("systemTable")
public record SystemTableInputSlice(String table, List<SystemTableSource> sources,
                                    @Nullable DimFilter filter,
                                    @Nullable List<String> columns,
                                    VirtualColumns virtualColumns,
                                    long limit) implements InputSlice
{
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
      @JsonProperty("limit") @Nullable final Long limit
  )
  {
    this(
        Preconditions.checkNotNull(table, "table"),
        List.copyOf(Preconditions.checkNotNull(sources, "sources")),
        filter,
        columns == null ? null : List.copyOf(columns),
        VirtualColumns.nullToEmpty(virtualColumns),
        limit == null ? Long.MAX_VALUE : limit
    );
  }

  @Override
  @JsonProperty
  public String table()
  {
    return table;
  }

  @Override
  @JsonProperty
  public List<SystemTableSource> sources()
  {
    return sources;
  }

  @Override
  @Nullable
  @JsonProperty
  public DimFilter filter()
  {
    return filter;
  }

  @Override
  @Nullable
  @JsonProperty
  public List<String> columns()
  {
    return columns;
  }

  @Override
  @JsonProperty
  public VirtualColumns virtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonProperty
  public long limit()
  {
    return limit;
  }

  @Override
  public int fileCount()
  {
    return sources.size();
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
