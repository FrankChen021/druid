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
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Collections;

/** Applies the same state-resource authorization as the bindable {@code sys.server_properties} implementation. */
public class NativeServerPropertiesTableRowAuthorizer implements NativeSystemTableRowAuthorizer
{
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public NativeServerPropertiesTableRowAuthorizer(final AuthorizerMapper authorizerMapper)
  {
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public Iterable<Object[]> filterAuthorizedRows(
      final Iterable<Object[]> rows,
      final AuthenticationResult authenticationResult
  )
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authorizationResult.getErrorMessage());
    }
    return rows;
  }
}
