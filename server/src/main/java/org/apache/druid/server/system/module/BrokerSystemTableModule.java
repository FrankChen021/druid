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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.system.table.ServersTableDataProvider;
import org.apache.druid.server.system.table.ServersTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;

import java.util.Properties;

/**
 * Registers the native system-table providers that only the Broker serves. They depend on Broker-only services, such
 * as the server inventory view, so they must not be bound on other nodes, where injecting the provider map would
 * construct them. Like {@link SystemTableModule}, it binds nothing unless
 * {@link SystemTableModule#ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY} is set.
 */
public class BrokerSystemTableModule implements Module
{
  private final boolean enabled;

  public BrokerSystemTableModule(final Properties properties)
  {
    this.enabled = SystemTableModule.isEnabled(properties);
  }

  @Override
  public void configure(final Binder binder)
  {
    if (!enabled) {
      return;
    }
    MapBinder.newMapBinder(binder, String.class, SystemTableDataProvider.class)
             .addBinding(ServersTableDescriptor.TABLE_NAME)
             .to(ServersTableDataProvider.class)
             .in(LazySingleton.class);
  }
}
