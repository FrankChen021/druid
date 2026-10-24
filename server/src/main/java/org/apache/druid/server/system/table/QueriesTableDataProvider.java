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
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Aggregates node-local query execution providers for the native {@code sys.queries} table. */
public class QueriesTableDataProvider implements SystemTableDataProvider
{
  private static final String ID = "id";
  private static final String ENGINE = "engine";
  private static final String INITIAL_QUERY_ID = "initial_query_id";
  private static final String SERVER = "server";
  private static final String SERVER_TYPE = "server_type";
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter(ID, null),
      new SystemTablePushdownFilter(ENGINE, null),
      new SystemTablePushdownFilter(INITIAL_QUERY_ID, null),
      new SystemTablePushdownFilter(SERVER, null),
      new SystemTablePushdownFilter(SERVER_TYPE, null)
  );

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
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  /**
   * Skips rows that can't match the pushed-down equality and {@code IN} filters before serializing them. The native
   * scan still applies the query's full filter, so other pushed-down filter types are ignored here.
   */
  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters)
  {
    final Map<String, Set<String>> allowedValues = getAllowedValues(filters);
    final List<Object[]> rows = new ArrayList<>();
    for (final SystemTableQueryInfoProvider queryInfoProvider : queryInfoProviders) {
      for (final SystemTableQueryInfo queryInfo : queryInfoProvider.getQueryInfo()) {
        if (matches(queryInfo, allowedValues)) {
          rows.add(toRow(queryInfo));
        }
      }
    }
    return rows;
  }

  /** Returns, for each filtered column, the values a row may have. Values of conjuncts on one column intersect. */
  private static Map<String, Set<String>> getAllowedValues(final List<DimFilter> filters)
  {
    final Map<String, Set<String>> allowedValues = new HashMap<>();
    for (final DimFilter filter : filters) {
      if (isStringValuesFilter(filter)) {
        final Set<String> values = new HashSet<>(SystemTablePushdownFilter.getStringValues(filter));
        allowedValues.merge(
            SystemTablePushdownFilter.getStringValuesColumn(filter),
            values,
            (existing, added) -> {
              existing.retainAll(added);
              return existing;
            }
        );
      }
    }
    return allowedValues;
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
  }

  private static boolean matches(final SystemTableQueryInfo queryInfo, final Map<String, Set<String>> allowedValues)
  {
    return matches(allowedValues, ID, queryInfo.id())
           && matches(allowedValues, ENGINE, queryInfo.engine())
           && matches(allowedValues, INITIAL_QUERY_ID, queryInfo.initialQueryId())
           && matches(allowedValues, SERVER, queryInfo.server())
           && matches(allowedValues, SERVER_TYPE, queryInfo.serverType());
  }

  private static boolean matches(
      final Map<String, Set<String>> allowedValues,
      final String column,
      @Nullable final String value
  )
  {
    final Set<String> values = allowedValues.get(column);
    return values == null || (value != null && values.contains(value));
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
