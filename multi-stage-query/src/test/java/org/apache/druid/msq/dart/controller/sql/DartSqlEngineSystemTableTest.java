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

import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

public class DartSqlEngineSystemTableTest
{
  /** The engine requires whatever the table's descriptor says, so the policy is defined in one place. */
  @Test
  public void testResourceActionsComeFromTheDescriptor()
  {
    final Set<ResourceAction> customActions =
        Set.of(new ResourceAction(new Resource("custom-resource", ResourceType.DATASOURCE), Action.WRITE));
    final SystemTableDescriptor customDescriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(customDescriptor.getResourceActions()).thenReturn(customActions);
    final DartSqlEngine engine = makeEngine(
        Map.of(
            ServerPropertiesTableDescriptor.TABLE_NAME, new ServerPropertiesTableDescriptor(),
            "custom", customDescriptor
        )
    );

    Assertions.assertEquals(
        Set.of(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        engine.getSystemTableDataSourceResourceActions(ServerPropertiesTableDescriptor.TABLE_NAME)
    );
    Assertions.assertEquals(customActions, engine.getSystemTableDataSourceResourceActions("custom"));
  }

  @Test
  public void testUnregisteredTableIsNotSupported()
  {
    final DartSqlEngine engine = makeEngine(Map.of());

    Assertions.assertFalse(engine.supportsSystemTableDataSource("unknown"));
    Assertions.assertEquals(Set.of(), engine.getSystemTableDataSourceResourceActions("unknown"));
  }

  private static DartSqlEngine makeEngine(final Map<String, SystemTableDescriptor> descriptors)
  {
    return new DartSqlEngine(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        descriptors
    );
  }
}
