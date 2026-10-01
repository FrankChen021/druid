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

package org.apache.druid.msq.dart.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.multibindings.OptionalBinder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ExecutorServices;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.query.DruidProcessingConfig;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

/** Installs the Dart worker runtime embedded in Brokers for system-table queries. */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class DartEmbeddedWorkerModule implements DruidModule
{
  @Inject
  private Properties properties;

  @Override
  public void configure(final Binder binder)
  {
    if (DartModules.isDartEnabled(properties)) {
      binder.install(DartWorkerModule.actualModule());
      OptionalBinder.newOptionalBinder(binder, Key.get(ExecutorService.class, Dart.class))
                    .setBinding()
                    .toProvider(EmbeddedProcessingExecutorProvider.class)
                    .in(LazySingleton.class);
    }
  }

  private static class EmbeddedProcessingExecutorProvider implements Provider<ExecutorService>
  {
    private final Lifecycle lifecycle;
    private final DruidProcessingConfig processingConfig;

    @Inject
    EmbeddedProcessingExecutorProvider(
        final Lifecycle lifecycle,
        final DruidProcessingConfig processingConfig
    )
    {
      this.lifecycle = lifecycle;
      this.processingConfig = processingConfig;
    }

    @Override
    public ExecutorService get()
    {
      return ExecutorServices.manageLifecycle(
          lifecycle,
          Execs.multiThreaded(processingConfig.getNumThreads(), "dart-embedded-processing-%s")
      );
    }
  }
}
