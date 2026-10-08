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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import jakarta.validation.Validation;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ConfigurationTableDataProvider;
import org.apache.druid.server.system.table.ConfigurationTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConfigurationTableDataProviderTest
{
  private static final AuthenticationResult AUTH = new AuthenticationResult("test", AuthConfig.ALLOW_ALL_NAME, null, null);
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final Properties properties = new Properties();
  private final AuthorizerMapper allowAll = authorizers((authentication, resource, action) -> Access.OK);

  @Test
  public void testUninitializedBindingsAreListedWithoutConstructingConfigs()
  {
    final JsonConfigProvider<ExampleConfig> configProvider = JsonConfigProvider.of("druid.example", ExampleConfig.class);
    final Injector injector = injector(binder -> binder.bind(ExampleConfig.class).toProvider(configProvider));
    final List<Object[]> rows = rows(provider(injector, allowAll));
    Assertions.assertNull(configProvider.getInitializedConfig());
    Assertions.assertEquals("NOT_INITIALIZED", find(rows, "druid.example.threads")[9]);
    Assertions.assertEquals("NOT_INITIALIZED", find(rows, "druid.example.nested.renamed")[9]);
    Assertions.assertTrue(rows.stream().noneMatch(row -> row[3].equals("druid.example.ignored")));
  }

  @Test
  public void testDefaultsAndConfiguredValuesUseGetters()
  {
    properties.setProperty("druid.example.threads", "0");
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.example", ExampleConfig.class));
    injector.getInstance(ExampleConfig.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    final Object[] threads = find(rows, "druid.example.threads");
    Assertions.assertEquals("0", threads[7]);
    Assertions.assertEquals("1", threads[8]);
    Assertions.assertEquals("AVAILABLE", threads[9]);
    final Object[] nested = find(rows, "druid.example.nested.renamed");
    Assertions.assertNull(nested[7]);
    Assertions.assertEquals("default", nested[8]);
    Assertions.assertEquals("localhost:8080", nested[0]);
    Assertions.assertEquals("overlord", nested[1]);
    Assertions.assertEquals("[overlord]", nested[2]);
  }

  @Test
  public void testRedactionHappensBeforeReadingValues()
  {
    properties.setProperty("druid.example.password", "secret");
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.example", ExampleConfig.class));
    injector.getInstance(ExampleConfig.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    for (final String name : List.of("password", "credentials")) {
      final Object[] row = find(rows, "druid.example." + name);
      Assertions.assertNull(row[7]);
      Assertions.assertNull(row[8]);
      Assertions.assertEquals("REDACTED", row[9]);
    }
    final Object[] map = find(rows, "druid.example.options");
    Assertions.assertEquals("UNSUPPORTED", map[9]);
    Assertions.assertNull(map[7]);
    Assertions.assertNull(map[8]);
  }

  @Test
  public void testInspectionErrorsDoNotExposeValues()
  {
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.example", ExampleConfig.class));
    injector.getInstance(ExampleConfig.class);
    final Object[] row = find(rows(provider(injector, allowAll)), "druid.example.broken");
    Assertions.assertEquals("ERROR", row[9]);
    Assertions.assertEquals("Unable to read configuration property", row[10]);
    Assertions.assertFalse(java.util.Arrays.toString(row).contains("secret"));
  }

  @Test
  public void testOnlyFinalBindingsAppearAndAnnotationsRemainDistinct()
  {
    final Module original = binder -> JsonConfigProvider.bind(binder, "druid.old", ExampleConfig.class);
    final Module replacement = binder -> {
      JsonConfigProvider.bind(binder, "druid.new", ExampleConfig.class);
      JsonConfigProvider.bind(binder, "druid.new", ExampleConfig.class, Names.named("other"));
    };
    final Injector injector = injector(Modules.override(original).with(replacement));
    injector.getInstance(ExampleConfig.class);
    injector.getInstance(Key.get(ExampleConfig.class, Names.named("other")));
    final List<Object[]> rows = rows(provider(injector, allowAll));
    Assertions.assertTrue(rows.stream().noneMatch(row -> ((String) row[3]).startsWith("druid.old")));
    final List<Object[]> threads = rows.stream().filter(row -> row[3].equals("druid.new.threads")).toList();
    Assertions.assertEquals(2, threads.size());
    Assertions.assertNotEquals(threads.get(0)[5], threads.get(1)[5]);
  }

  @Test
  public void testParentInjectorBindingsAreIncludedOnce()
  {
    final Injector parent = injector(binder -> JsonConfigProvider.bind(binder, "druid.parent", ExampleConfig.class));
    final Injector child = parent.createChildInjector(
        binder -> JsonConfigProvider.bind(binder, "druid.child", NestedConfig.class)
    );
    final List<Object[]> rows = rows(provider(child, allowAll));
    Assertions.assertEquals(1, rows.stream().filter(row -> row[3].equals("druid.parent.threads")).count());
    Assertions.assertEquals(1, rows.stream().filter(row -> row[3].equals("druid.child.renamed")).count());
  }

  @Test
  public void testFiltersPruneNodes()
  {
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.example", ExampleConfig.class));
    final ConfigurationTableDataProvider provider = provider(injector, allowAll);
    Assertions.assertFalse(provider.getRows(List.of(new SelectorDimFilter("server", "other", null)), AUTH).iterator().hasNext());
    Assertions.assertFalse(provider.getRows(List.of(new EqualityFilter("service_name", ColumnType.STRING, "broker", null)), AUTH).iterator().hasNext());
    Assertions.assertTrue(provider.getRows(List.of(new SelectorDimFilter("service_name", "overlord", null)), AUTH).iterator().hasNext());
  }

  @Test
  public void testConfigReadRequiredAtProviderAndBrokerEvenForEmptyRows()
  {
    final AuthorizerMapper stateOnly = authorizers(
        (authentication, resource, action) -> ResourceType.STATE.equals(resource.getType()) ? Access.OK : Access.DENIED
    );
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.example", ExampleConfig.class));
    Assertions.assertThrows(ForbiddenException.class, () -> rows(provider(injector, stateOnly)));
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> new ConfigurationTableDescriptor().getRowAuthorizer().filterAuthorizedRows(List.of(), AUTH, stateOnly)
    );
  }

  private Injector injector(final Module module)
  {
    return Guice.createInjector(new DruidGuiceExtensions(), binder -> {
      binder.bind(Properties.class).toInstance(properties);
      binder.bind(JsonConfigurator.class).toInstance(new JsonConfigurator(
          mapper,
          Validation.buildDefaultValidatorFactory().getValidator()
      ));
    }, module);
  }

  private ConfigurationTableDataProvider provider(final Injector injector, final AuthorizerMapper authorizers)
  {
    return new ConfigurationTableDataProvider(
        injector,
        mapper,
        properties,
        new DruidNode("overlord", "localhost", false, 8080, null, true, false),
        Set.of(NodeRole.OVERLORD),
        new DruidServerConfig(null, null),
        authorizers
    );
  }

  private static AuthorizerMapper authorizers(final Authorizer authorizer)
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(final String name)
      {
        return authorizer;
      }
    };
  }

  private static List<Object[]> rows(final ConfigurationTableDataProvider provider)
  {
    final List<Object[]> rows = new ArrayList<>();
    provider.getRows(List.of(), AUTH).forEach(rows::add);
    return rows;
  }

  private static Object[] find(final List<Object[]> rows, final String property)
  {
    return rows.stream().filter(row -> property.equals(row[3])).findFirst().orElseThrow();
  }

  public static class ExampleConfig
  {
    @JsonProperty
    private int threads = 5;
    @JsonProperty
    private final NestedConfig nested = new NestedConfig();
    @JsonProperty
    private String password;
    @JsonProperty
    private final PasswordProvider credentials = () -> {
      throw new AssertionError("Do not resolve credentials");
    };
    @JsonProperty
    private final Map<String, String> options = Map.of("password", "secret");
    @JsonProperty
    private final String broken = "unused";
    @JsonIgnore
    private final String ignored = "unused";

    public int getThreads()
    {
      return Math.max(1, threads);
    }

    public String getPassword()
    {
      throw new AssertionError("Do not read hidden property");
    }

    public String getBroken()
    {
      throw new IllegalStateException("secret");
    }
  }

  public static class NestedConfig
  {
    @JsonProperty("renamed")
    private final String value = "default";
  }
}
