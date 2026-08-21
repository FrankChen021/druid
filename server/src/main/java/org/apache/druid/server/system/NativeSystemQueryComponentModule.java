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

package org.apache.druid.server.system;

import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.log.NoopRequestLoggerProvider;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;

import java.util.List;

/** Installs the minimum query dependencies required by a component-local native system-table endpoint. */
public class NativeSystemQueryComponentModule implements DruidModule
{
  @Override
  public void configure(final Binder binder)
  {
    DruidBinders.dataSourceQueryHandlerBinder(binder);
    binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bindWithDefault(
        binder,
        "druid.request.logging",
        RequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    );

    binder.bind(GenericQueryMetricsFactory.class)
          .to(DefaultGenericQueryMetricsFactory.class)
          .in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.query.default", DefaultQueryConfig.class);
    binder.bind(QueryConfigProvider.class).to(DefaultQueryConfig.class);
    binder.bind(ScanQueryQueryToolChest.class).in(LazySingleton.class);
    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(NativeSystemQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);
    binder.bind(QueryScheduler.class)
          .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
          .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.query.scheduler", QuerySchedulerProvider.class, Global.class);
  }

  @Override
  public List<com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return new QueryableModule().getJacksonModules();
  }
}
