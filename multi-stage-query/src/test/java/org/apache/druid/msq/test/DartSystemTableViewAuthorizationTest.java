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


package org.apache.druid.msq.test;

import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Dart workers read system-table rows with the internal identity, so the caller's STATE READ for
 * {@code sys.server_properties} is checked when the SQL is planned. Statement authorization only sees the view named in
 * the SQL, so these tests check that a view over the table, wherever it appears, still requires STATE READ.
 */
@SqlTestFrameworkConfig.ComponentSupplier(DartSystemTableViewAuthorizationTest.SystemTableViewComponentSupplier.class)
public class DartSystemTableViewAuthorizationTest extends BaseCalciteQueryTest
{
  private static final String VIEW_NAME = "serverPropertiesView";

  @Test
  public void testViewInFromRequiresStateRead()
  {
    assertQueryIsForbidden(
        "SELECT property FROM view." + VIEW_NAME,
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
  }

  @Test
  public void testViewInInSubqueryRequiresStateRead()
  {
    assertQueryIsForbidden(
        "SELECT dim1 FROM foo WHERE dim1 IN (SELECT property FROM view." + VIEW_NAME + ")",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
  }

  @Test
  public void testViewInScalarSubqueryRequiresStateRead()
  {
    assertQueryIsForbidden(
        "SELECT dim1, (SELECT COUNT(*) FROM view." + VIEW_NAME + ") FROM foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
  }

  /** A caller with STATE READ passes the check, so the rejections above are permission checks, not planning failures. */
  @Test
  public void testViewInSubqueryIsAllowedWithStateRead()
  {
    testBuilder()
        .sql("EXPLAIN PLAN FOR SELECT dim1 FROM foo WHERE dim1 IN (SELECT property FROM view." + VIEW_NAME + ")")
        .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
        .run();
  }

  /** Dart test components with the {@code server_properties} descriptor and a view over the table. */
  public static class SystemTableViewComponentSupplier extends DartComponentSupplier
  {
    public SystemTableViewComponentSupplier(final TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new ServerPropertiesDescriptorModule());
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      final PlannerComponentSupplier delegate = super.getPlannerComponentSupplier();
      return new PlannerComponentSupplier()
      {
        @Override
        public Set<ExtensionCalciteRuleProvider> extensionCalciteRules()
        {
          return delegate.extensionCalciteRules();
        }

        @Override
        public ViewManager createViewManager()
        {
          return delegate.createViewManager();
        }

        @Override
        public void populateViews(final ViewManager viewManager, final PlannerFactory plannerFactory)
        {
          delegate.populateViews(viewManager, plannerFactory);
          viewManager.createView(
              plannerFactory,
              VIEW_NAME,
              "SELECT server, property, \"value\" FROM sys.server_properties"
          );
        }

        @Override
        public DruidSchemaManager createSchemaManager()
        {
          return delegate.createSchemaManager();
        }

        @Override
        public void finalizePlanner(final PlannerFixture plannerFixture)
        {
          delegate.finalizePlanner(plannerFixture);
        }
      };
    }
  }

  private static class ServerPropertiesDescriptorModule implements DruidModule
  {
    @Override
    public void configure(final Binder binder)
    {
      MapBinder.newMapBinder(binder, String.class, SystemTableDescriptor.class)
               .addBinding(ServerPropertiesTableDescriptor.TABLE_NAME)
               .toInstance(new ServerPropertiesTableDescriptor());
    }
  }
}
