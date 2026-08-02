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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Versioned task-list endpoint whose filters are applied before the completed-task limit.
 */
@Path("/druid/indexer/v2/tasks")
public class OverlordTaskResourceV2
{
  private final OverlordResource overlordResource;

  @Inject
  public OverlordTaskResourceV2(final OverlordResource overlordResource)
  {
    this.overlordResource = overlordResource;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTasks(
      @QueryParam("state") final String state,
      @QueryParam("datasource") final String dataSource,
      @QueryParam("createdTimeInterval") final String createdTimeInterval,
      @QueryParam("max") final Integer maxCompletedTasks,
      @QueryParam("type") final String type,
      @QueryParam("taskId") final String taskId,
      @QueryParam("groupId") final String groupId,
      @Context final HttpServletRequest req
  )
  {
    return overlordResource.getTasks(
        state,
        dataSource,
        createdTimeInterval,
        maxCompletedTasks,
        type,
        taskId,
        groupId,
        req
    );
  }
}
