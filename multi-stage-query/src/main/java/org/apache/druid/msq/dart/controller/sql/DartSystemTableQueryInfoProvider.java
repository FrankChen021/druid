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

package org.apache.druid.msq.dart.controller.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.QueryInfoAndReport;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.server.system.table.SystemTableQueryInfo;
import org.apache.druid.server.system.table.SystemTableQueryInfoProvider;

import java.util.ArrayList;
import java.util.List;

/** Contributes node-local Dart controller executions to the native {@code sys.queries} table. */
public class DartSystemTableQueryInfoProvider implements SystemTableQueryInfoProvider
{
  private static final String SERVER_TYPE = "broker";

  private final DartControllerRegistry controllerRegistry;
  private final ObjectMapper jsonMapper;

  @Inject
  public DartSystemTableQueryInfoProvider(
      final DartControllerRegistry controllerRegistry,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.controllerRegistry = controllerRegistry;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Iterable<SystemTableQueryInfo> getQueryInfo()
  {
    final List<QueryInfoAndReport> queryDetails = controllerRegistry.getAllQueryDetails(true);
    final List<SystemTableQueryInfo> queryInfo = new ArrayList<>(queryDetails.size());
    for (final QueryInfoAndReport queryDetail : queryDetails) {
      final DartQueryInfo dartQueryInfo = queryDetail.getQueryInfo();
      queryInfo.add(
          new SystemTableQueryInfo(
              dartQueryInfo.executionId(),
              dartQueryInfo.engine(),
              dartQueryInfo.state(),
              serializeInfo(dartQueryInfo),
              dartQueryInfo.getSqlQueryId(),
              true,
              dartQueryInfo.getControllerHost(),
              SERVER_TYPE
          )
      );
    }
    return queryInfo;
  }

  private String serializeInfo(final DartQueryInfo queryInfo)
  {
    try {
      return jsonMapper.writeValueAsString(queryInfo);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
