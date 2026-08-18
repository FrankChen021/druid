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
import org.apache.druid.server.NativeSystemTableRowAuthorizer;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.Collections;

/** Applies the same per-datasource authorization as the bindable {@code sys.tasks} implementation. */
public class NativeTasksTableRowAuthorizer implements NativeSystemTableRowAuthorizer
{
  private static final int DATASOURCE_COLUMN = 3;

  private final AuthorizerMapper authorizerMapper;

  @Inject
  public NativeTasksTableRowAuthorizer(final AuthorizerMapper authorizerMapper)
  {
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public Iterable<Object[]> filterAuthorizedRows(
      final Iterable<Object[]> rows,
      final AuthenticationResult authenticationResult
  )
  {
    return AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        rows,
        row -> Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply((String) row[DATASOURCE_COLUMN])
        ),
        authorizerMapper
    );
  }
}
