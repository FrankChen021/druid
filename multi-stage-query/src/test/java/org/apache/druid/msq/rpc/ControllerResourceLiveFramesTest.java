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

package org.apache.druid.msq.rpc;

import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.LiveFramesSession;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/// Checks owner isolation and permitted operations on the internal remote-frame controller API.
public class ControllerResourceLiveFramesTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder().add("value", ColumnType.LONG).build();

  @Test
  public void testAllSessionOperationsRequireTheOwningIdentity()
  {
    final Controller controller = mock(Controller.class);
    final LiveFramesSession session = new LiveFramesSession("query-1", "owner");
    session.markRunning();
    session.markReady(
        new StageId("query-1", 2),
        SIGNATURE,
        List.of(new LiveFramesSession.PartitionLocation("worker-1", 0))
    );
    final LiveFramesSession.SessionInfo info = session.getInfo();
    final String partitionId = info.getManifest().getPartitions().get(0).getId();
    final LiveFramesSession.PartitionLocation location = session.beginPartitionRead(partitionId);
    when(controller.getLiveFramesSessionInfo()).thenReturn(info);
    when(controller.isLiveFramesSessionOwnedBy("owner")).thenReturn(true);
    when(controller.isLiveFramesSessionOwnedBy("other")).thenReturn(false);
    when(controller.beginLiveFramesPartitionRead(anyString())).thenReturn(location);
    when(controller.renewLiveFramesLease(60_000)).thenReturn(true);

    final ResourcePermissionMapper permissionMapper = new ResourcePermissionMapper()
    {
      @Override
      public List<ResourceAction> getAdminPermissions()
      {
        return List.of();
      }

      @Override
      public List<ResourceAction> getQueryPermissions(final String queryId)
      {
        return List.of();
      }
    };
    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        Map.of(AuthConfig.ALLOW_ALL_NAME, new AllowAllAuthorizer(null))
    );
    final ControllerResource resource = new ControllerResource(controller, permissionMapper, authorizerMapper);
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .thenReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT);

    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.httpGetLiveFramesStatus("query-1", "other", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.httpRenewLiveFramesLease(new ControllerResource.LiveFramesLeaseRequest("other", 60_000), "query-1", request)
                .getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.httpGetLiveFramesPartitionLocation("query-1", partitionId, "other", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.httpEndLiveFramesPartitionRead("query-1", location.getReadId(), "other", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.httpReleaseLiveFramesSession("query-1", "other", request).getStatus()
    );
    verify(controller, never()).renewLiveFramesLease(60_000);
    verify(controller, never()).beginLiveFramesPartitionRead(anyString());
    verify(controller, never()).endLiveFramesPartitionRead(anyString());
    verify(controller, never()).releaseLiveFramesSession();

    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.httpGetLiveFramesStatus("query-1", "owner", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.httpRenewLiveFramesLease(new ControllerResource.LiveFramesLeaseRequest("owner", 60_000), "query-1", request)
                .getStatus()
    );
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.httpGetLiveFramesPartitionLocation("query-1", partitionId, "owner", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.httpEndLiveFramesPartitionRead("query-1", location.getReadId(), "owner", request).getStatus()
    );
    Assertions.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.httpReleaseLiveFramesSession("query-1", "owner", request).getStatus()
    );
    verify(controller).renewLiveFramesLease(60_000);
    verify(controller).beginLiveFramesPartitionRead(partitionId);
    verify(controller).endLiveFramesPartitionRead(location.getReadId());
    verify(controller).releaseLiveFramesSession();
  }
}
