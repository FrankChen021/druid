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

package org.apache.druid.server.system.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.system.table.ServersTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Properties;

public class BrokerSystemTableModuleTest
{
  /**
   * With the Druid engine enabled for system tables, the Broker serves {@code sys.servers}: both its descriptor, which
   * authorizes and routes queries, and its provider are bound.
   */
  @Test
  public void testBindsServersTableWhenEnabled()
  {
    final Properties properties = new Properties();
    properties.setProperty(SystemTableModule.ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY, "true");
    final Injector injector = injector(properties);

    final Map<String, SystemTableDescriptor> descriptors =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, SystemTableDescriptor>>() {}));
    Assertions.assertInstanceOf(ServersTableDescriptor.class, descriptors.get(ServersTableDescriptor.TABLE_NAME));
    final Map<String, Provider<SystemTableDataProvider>> providers =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, Provider<SystemTableDataProvider>>>() {}));
    Assertions.assertTrue(providers.containsKey(ServersTableDescriptor.TABLE_NAME));
  }

  /**
   * Without the property, the module binds nothing.
   */
  @Test
  public void testBindsNothingWhenDisabled()
  {
    final Injector injector = injector(new Properties());

    Assertions.assertNull(
        injector.getExistingBinding(Key.get(new TypeLiteral<Map<String, SystemTableDescriptor>>() {}))
    );
    Assertions.assertNull(
        injector.getExistingBinding(Key.get(new TypeLiteral<Map<String, SystemTableDataProvider>>() {}))
    );
  }

  private static Injector injector(final Properties properties)
  {
    return Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(DruidNodeDiscoveryProvider.class).toInstance(Mockito.mock(DruidNodeDiscoveryProvider.class));
          binder.bind(FilteredServerInventoryView.class).toInstance(Mockito.mock(FilteredServerInventoryView.class));
          binder.bind(OverlordClient.class).toInstance(Mockito.mock(OverlordClient.class));
          binder.bind(CoordinatorClient.class).toInstance(Mockito.mock(CoordinatorClient.class));
          binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(new DefaultObjectMapper());
        },
        new BrokerSystemTableModule(properties)
    );
  }
}
