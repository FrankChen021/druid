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

package org.apache.druid.indexing.overlord.http;

import com.google.inject.Inject;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.NativeSystemQueryClient;
import org.apache.druid.server.security.AuthenticationResult;

/** Separates original Broker fanout from component-local requests received through the Broker's native endpoint. */
public class NativeSystemTableBrokerQueryHandler implements DataSourceQueryHandler
{
  private final NativeSystemQueryClient systemQueryClient;
  private final NativeSystemTableQueryHandler localQueryHandler;

  @Inject
  public NativeSystemTableBrokerQueryHandler(
      final NativeSystemQueryClient systemQueryClient,
      final NativeSystemTableQueryHandler localQueryHandler
  )
  {
    this.systemQueryClient = systemQueryClient;
    this.localQueryHandler = localQueryHandler;
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult authenticationResult
  )
  {
    if (query.context().getBoolean(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_COMPONENT_LOCAL, false)) {
      return localQueryHandler.createRunner(query, authenticationResult);
    }
    return systemQueryClient.createRunner(query, authenticationResult);
  }
}
