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
import org.apache.druid.client.cache.CacheProvider;
import org.apache.druid.client.cache.CaffeineCacheConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.metadata.MetadataRuleManagerConfig;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.log.NoopRequestLoggerProvider;
import org.apache.druid.server.log.RequestLoggerProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.ConfigurationTableDataProvider;
import org.apache.druid.server.system.table.ConfigurationTableDescriptor;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
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
  public void testInitializedNullNestedBeanHasAvailableNullLeaves()
  {
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.nullNested", NullNestedConfig.class));
    final List<Object[]> before = rows(provider(injector, allowAll));
    Assertions.assertEquals("NOT_INITIALIZED", find(before, "druid.nullNested.nested.renamed")[9]);
    injector.getInstance(NullNestedConfig.class);
    assertValue(rows(provider(injector, allowAll)), "druid.nullNested.nested.renamed", null, null, String.class);
  }

  @Test
  public void testPolymorphicTypeSelectorBeforeAndAfterInitialization()
  {
    properties.setProperty("druid.cache.type", "caffeine");
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.cache", CacheProvider.class));
    final Object[] before = find(rows(provider(injector, allowAll)), "druid.cache.type");
    Assertions.assertEquals("caffeine", before[7]);
    Assertions.assertNull(before[8]);
    Assertions.assertEquals("NOT_INITIALIZED", before[9]);
    injector.getInstance(CacheProvider.class);
    assertValue(rows(provider(injector, allowAll)), "druid.cache.type", "caffeine", "caffeine", String.class);
  }

  @Test
  public void testDefaultPolymorphicTypeWithoutBeanProperties()
  {
    mapper.registerSubtypes(NoopRequestLoggerProvider.class);
    final Injector injector = injector(binder -> JsonConfigProvider.bindWithDefault(
        binder,
        "druid.request.logging",
        RequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    ));
    injector.getInstance(RequestLoggerProvider.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    assertValue(rows, "druid.request.logging.type", null, "noop", String.class);
    Assertions.assertTrue(rows.stream().noneMatch(row -> "druid.request.logging".equals(row[3])));
  }

  @Test
  public void testEffectiveValuesAreTypedJsonAndScalarCollections()
  {
    properties.setProperty("druid.json.names", "[\"alpha\",\"beta\"]");
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.json", JsonValuesConfig.class));
    injector.getInstance(JsonValuesConfig.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    Assertions.assertEquals(ColumnType.NESTED_DATA, ConfigurationTableDescriptor.ROW_SIGNATURE.getColumnType("effective_value").orElseThrow());
    assertJsonValue(rows, "names", List.of("alpha", "beta"));
    Assertions.assertEquals("[\"alpha\",\"beta\"]", find(rows, "druid.json.names")[7]);
    assertJsonValue(rows, "numbers", List.of(1, 2));
    assertJsonValue(rows, "enabled", true);
    assertJsonValue(rows, "count", 3);
    assertJsonValue(rows, "letter", "a");
    assertJsonValue(rows, "empty", List.of());
    assertJsonValue(rows, "nullable", null);
    assertJsonValue(rows, "bytes", List.of("2.00 MiB", "-1 B"));
    Assertions.assertEquals("UNSUPPORTED", find(rows, "druid.json.objects")[9]);
  }

  @Test
  public void testCollectionsRemainUninitializedAndRedactedBeforeAccess()
  {
    final JsonConfigProvider<JsonValuesConfig> configProvider = JsonConfigProvider.of("druid.json", JsonValuesConfig.class);
    final Injector injector = injector(binder -> binder.bind(JsonValuesConfig.class).toProvider(configProvider));
    final List<Object[]> before = rows(provider(injector, allowAll));
    Assertions.assertNull(configProvider.getInitializedConfig());
    Assertions.assertEquals("NOT_INITIALIZED", find(before, "druid.json.names")[9]);
    configProvider.get();
    final List<Object[]> after = rows(provider(injector, allowAll));
    final Object[] hidden = find(after, "druid.json.passwords");
    Assertions.assertEquals("REDACTED", hidden[9]);
    Assertions.assertNull(hidden[8]);
    Assertions.assertEquals("UNSUPPORTED", find(after, "druid.json.credentials")[9]);
    assertJsonValue(after, "names", List.of());
  }

  private static void assertJsonValue(final List<Object[]> rows, final String name, final Object expected)
  {
    final Object[] row = find(rows, "druid.json." + name);
    Assertions.assertEquals(expected, StructuredData.unwrap(row[8]));
    if (expected != null) {
      Assertions.assertInstanceOf(StructuredData.class, row[8]);
    }
    Assertions.assertEquals("AVAILABLE", row[9]);
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
    Assertions.assertEquals(1, StructuredData.unwrap(threads[8]));
    Assertions.assertEquals("AVAILABLE", threads[9]);
    final Object[] nested = find(rows, "druid.example.nested.renamed");
    Assertions.assertNull(nested[7]);
    Assertions.assertEquals("default", StructuredData.unwrap(nested[8]));
    Assertions.assertEquals("localhost:8080", nested[0]);
    Assertions.assertEquals("overlord", nested[1]);
    Assertions.assertEquals("[overlord]", nested[2]);
  }

  @Test
  public void testCommonValueTypesInRealConfigurationClasses()
  {
    properties.setProperty("druid.coordinator.loadqueuepeon.http.hostTimeout", "PT2M");
    properties.setProperty("druid.manager.rules.pollDuration", "P1M");
    properties.setProperty("druid.cache.sizeInBytes", "2MiB");
    final Injector injector = injector(binder -> {
      JsonConfigProvider.bind(binder, "druid.coordinator.loadqueuepeon.http", HttpLoadQueuePeonConfig.class);
      JsonConfigProvider.bind(binder, "druid.manager.rules", MetadataRuleManagerConfig.class);
      JsonConfigProvider.bind(binder, "druid.cache", CaffeineCacheConfig.class);
    });
    injector.getInstance(HttpLoadQueuePeonConfig.class);
    injector.getInstance(MetadataRuleManagerConfig.class);
    injector.getInstance(CaffeineCacheConfig.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    assertValue(rows, "druid.coordinator.loadqueuepeon.http.hostTimeout", "PT2M", "PT120S", Duration.class);
    assertValue(rows, "druid.coordinator.loadqueuepeon.http.repeatDelay", null, "PT60S", Duration.class);
    assertValue(rows, "druid.manager.rules.pollDuration", "P1M", "P1M", Period.class);
    assertValue(rows, "druid.manager.rules.alertThreshold", null, "PT10M", Period.class);
    assertValue(rows, "druid.cache.sizeInBytes", "2MiB", "2.00 MiB", HumanReadableBytes.class);
  }

  @Test
  public void testValueTypesReadFieldsAndPreserveNullValues()
  {
    final Injector injector = injector(binder -> JsonConfigProvider.bind(binder, "druid.values", ValueConfig.class));
    injector.getInstance(ValueConfig.class);
    final List<Object[]> rows = rows(provider(injector, allowAll));
    assertValue(rows, "druid.values.duration", null, "PT0S", Duration.class);
    assertValue(rows, "druid.values.period", null, "PT0S", Period.class);
    assertValue(rows, "druid.values.bytes", null, "-1 B", HumanReadableBytes.class);
    assertValue(rows, "druid.values.nullDuration", null, null, Duration.class);
    assertValue(rows, "druid.values.nullPeriod", null, null, Period.class);
    assertValue(rows, "druid.values.nullBytes", null, null, HumanReadableBytes.class);
  }

  @Test
  public void testValueTypesRemainUninitializedAndRedacted()
  {
    final JsonConfigProvider<ValueConfig> configProvider = JsonConfigProvider.of("druid.values", ValueConfig.class);
    final Injector injector = injector(binder -> binder.bind(ValueConfig.class).toProvider(configProvider));
    final List<Object[]> rows = rows(provider(injector, allowAll));
    Assertions.assertNull(configProvider.getInitializedConfig());
    for (final String name : List.of("duration", "period", "bytes")) {
      final Object[] row = find(rows, "druid.values." + name);
      Assertions.assertEquals("NOT_INITIALIZED", row[9]);
      Assertions.assertNull(row[8]);
    }
    for (final String name : List.of("passwordDuration", "tokenPeriod", "keyBytes")) {
      final Object[] row = find(rows, "druid.values." + name);
      Assertions.assertEquals("REDACTED", row[9]);
      Assertions.assertNull(row[7]);
      Assertions.assertNull(row[8]);
    }
  }

  private static void assertValue(
      final List<Object[]> rows,
      final String property,
      final String configured,
      final Object effective,
      final Class<?> type
  )
  {
    final Object[] row = find(rows, property);
    Assertions.assertEquals(type.getName(), row[6]);
    Assertions.assertEquals(configured, row[7]);
    Assertions.assertEquals(effective, StructuredData.unwrap(row[8]));
    Assertions.assertEquals("AVAILABLE", row[9]);
    Assertions.assertTrue(rows.stream().noneMatch(r -> ((String) r[3]).startsWith(property + ".")));
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

  public static class ValueConfig
  {
    @JsonProperty
    private final Duration duration = Duration.ZERO;
    @JsonProperty
    private final Period period = Period.ZERO;
    @JsonProperty
    private final HumanReadableBytes bytes = HumanReadableBytes.valueOf(-1);
    @JsonProperty
    private final Duration nullDuration = null;
    @JsonProperty
    private final Period nullPeriod = null;
    @JsonProperty
    private final HumanReadableBytes nullBytes = null;
    @JsonProperty
    private final Duration passwordDuration = Duration.ZERO;
    @JsonProperty
    private final Period tokenPeriod = Period.ZERO;
    @JsonProperty
    private final HumanReadableBytes keyBytes = HumanReadableBytes.ZERO;
  }

  public static class JsonValuesConfig
  {
    @JsonProperty
    private final Set<String> names = new LinkedHashSet<>();
    @JsonProperty
    private final int[] numbers = {1, 2};
    @JsonProperty
    private final boolean enabled = true;
    @JsonProperty
    private final int count = 3;
    @JsonProperty
    private final char letter = 'a';
    @JsonProperty
    private final List<String> empty = List.of();
    @JsonProperty
    private final List<String> nullable = null;
    @JsonProperty
    private final List<HumanReadableBytes> bytes = List.of(new HumanReadableBytes("2MiB"), HumanReadableBytes.valueOf(-1));
    @JsonProperty
    private final List<Object> objects = List.of(new Object());
    @JsonProperty
    private final List<String> passwords = List.of("secret");
    @JsonProperty
    private final Set<PasswordProvider> credentials = Set.of(() -> {
      throw new AssertionError("Do not resolve credentials");
    });

    public List<String> getPasswords()
    {
      throw new AssertionError("Do not read hidden collection");
    }
  }

  public static class NullNestedConfig
  {
    @JsonProperty
    private final NestedConfig nested = null;
  }

  public static class NestedConfig
  {
    @JsonProperty("renamed")
    private final String value = "default";
  }
}
