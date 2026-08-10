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
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;

import java.util.Set;

/**
 * Registers the component-side native system-table row endpoint and its data suppliers. The module has no local
 * query-engine dependencies: components return storage-prefiltered rows and the Broker executes the original native
 * query over the combined rows. The task supplier is registered only when {@link NodeRole#OVERLORD} is present.
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

    Jerseys.addResource(binder, NativeSystemQueryResource.class);
  }
}
