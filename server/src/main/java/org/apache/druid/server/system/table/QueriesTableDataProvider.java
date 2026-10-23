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

package org.apache.druid.server.system.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Aggregates node-local query execution providers for the native {@code sys.queries} table. */
public class QueriesTableDataProvider implements SystemTableDataProvider
{
  private final Set<SystemTableQueryInfoProvider> queryInfoProviders;
  private final ObjectMapper jsonMapper;

  @Inject
  public QueriesTableDataProvider(
      final Set<SystemTableQueryInfoProvider> queryInfoProviders,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.queryInfoProviders = queryInfoProviders;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters)
  {
    final List<Object[]> rows = new ArrayList<>();
    for (final SystemTableQueryInfoProvider queryInfoProvider : queryInfoProviders) {
      for (final SystemTableQueryInfo queryInfo : queryInfoProvider.getQueryInfo()) {
        rows.add(toRow(queryInfo));
      }
    }
    return rows;
  }

  private Object[] toRow(final SystemTableQueryInfo queryInfo)
  {
    return new Object[]{
        queryInfo.id(),
        queryInfo.engine(),
        queryInfo.state(),
        toJson(queryInfo.info()),
        queryInfo.initialQueryId(),
        queryInfo.initialQuery() ? 1L : 0L,
        queryInfo.server(),
        queryInfo.serverType()
    };
  }

  @Nullable
  private String toJson(@Nullable final Map<String, Object> info)
  {
    if (info == null) {
      return null;
    }
    try {
      return jsonMapper.writeValueAsString(info);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Failed to serialize query info");
    }
  }
}
