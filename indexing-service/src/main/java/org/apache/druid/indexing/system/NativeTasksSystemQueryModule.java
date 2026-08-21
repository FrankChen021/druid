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

package org.apache.druid.indexing.system;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.overlord.http.NativeTasksTableSupplier;
import org.apache.druid.server.system.NativeSystemTableDataSupplier;
import org.apache.druid.server.system.NativeSystemTableDescriptor;
import org.apache.druid.server.system.NativeSystemTableRowAuthorizer;
import org.apache.druid.server.system.NativeTasksTableRowAuthorizer;

import java.util.Set;

/** Registers the native {@code sys.tasks} table and its Overlord-local supplier. */
public class NativeTasksSystemQueryModule implements Module
{
  private Set<NodeRole> nodeRoles;

  @Inject
  public void configure(@Self final Set<NodeRole> nodeRoles)
  {
    this.nodeRoles = nodeRoles;
  }

  @Override
  public void configure(final Binder binder)
  {
    final MapBinder<String, NativeSystemTableDescriptor> descriptorBinder =
        MapBinder.newMapBinder(binder, String.class, NativeSystemTableDescriptor.class);
    final NativeSystemTableDescriptor tasksDescriptor = new NativeSystemTableDescriptor(
        Set.of(NodeRole.OVERLORD),
        NativeTasksTableSupplier.ROW_SIGNATURE
    );
    descriptorBinder.addBinding(NativeTasksTableSupplier.TABLE_NAME)
                    .toInstance(tasksDescriptor);

    final MapBinder<String, NativeSystemTableRowAuthorizer> rowAuthorizerBinder =
        MapBinder.newMapBinder(binder, String.class, NativeSystemTableRowAuthorizer.class);
    rowAuthorizerBinder.addBinding(NativeTasksTableSupplier.TABLE_NAME)
                       .to(NativeTasksTableRowAuthorizer.class)
                       .in(LazySingleton.class);

    if (nodeRoles.contains(NodeRole.OVERLORD)) {
      final MapBinder<String, NativeSystemTableDataSupplier> dataSupplierBinder =
          MapBinder.newMapBinder(binder, String.class, NativeSystemTableDataSupplier.class);
      dataSupplierBinder.addBinding(NativeTasksTableSupplier.TABLE_NAME)
                        .to(NativeTasksTableSupplier.class)
                        .in(LazySingleton.class);
    }
  }
}
