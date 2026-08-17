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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.NativeSystemTableDescriptor;

import java.util.Set;

/**
 * Registers native system-table routing and component-local row suppliers. The task supplier is registered only when
 * {@link NodeRole#OVERLORD} is present.
 */
public class NativeSystemQueryModule implements Module
{
  private Set<NodeRole> nodeRoles;

  public NativeSystemQueryModule()
  {
  }

  @Inject
  public void configure(@Self final Set<NodeRole> nodeRoles)
  {
    this.nodeRoles = nodeRoles;
  }

  @Override
  public void configure(final Binder binder)
  {
    DruidBinders.dataSourceQueryHandlerBinder(binder)
                .addBinding(SystemTableDataSource.class)
                .to(
                    nodeRoles.contains(NodeRole.BROKER)
                    ? NativeSystemTableBrokerQueryHandler.class
                    : NativeSystemTableQueryHandler.class
                )
                .in(LazySingleton.class);

    final MapBinder<String, NativeSystemTableDescriptor> descriptorBinder =
        MapBinder.newMapBinder(binder, String.class, NativeSystemTableDescriptor.class);
    descriptorBinder.addBinding(NativeServerPropertiesTableSupplier.TABLE_NAME)
                    .toInstance(new NativeSystemTableDescriptor(Set.of(NodeRole.values())));
    descriptorBinder.addBinding(NativeTasksTableSupplier.TABLE_NAME)
                    .toInstance(new NativeSystemTableDescriptor(Set.of(NodeRole.OVERLORD)));

    final MapBinder<String, NativeSystemTableDataSupplier> dataSupplierBinder =
        MapBinder.newMapBinder(binder, String.class, NativeSystemTableDataSupplier.class);
    dataSupplierBinder.addBinding(NativeServerPropertiesTableSupplier.TABLE_NAME)
                  .to(NativeServerPropertiesTableSupplier.class)
                  .in(LazySingleton.class);
    if (nodeRoles.contains(NodeRole.OVERLORD)) {
      dataSupplierBinder.addBinding(NativeTasksTableSupplier.TABLE_NAME)
                    .to(NativeTasksTableSupplier.class)
                    .in(LazySingleton.class);
    }
  }
}
