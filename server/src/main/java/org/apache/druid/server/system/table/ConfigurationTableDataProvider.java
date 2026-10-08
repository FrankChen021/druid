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

package org.apache.druid.server.system.table;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.spi.ProviderInstanceBinding;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Discovers installed JsonConfigProvider bindings without provisioning them. Scalar values are read from the
 * already initialized objects; arbitrary object serializers and secret providers are never invoked.
 */
public class ConfigurationTableDataProvider implements SystemTableDataProvider
{
  private final Injector injector;
  private final ObjectMapper mapper;
  private final ObjectMapper accessorMapper;
  private final Properties properties;
  private final DruidNode node;
  private final String nodeRoles;
  private final DruidServerConfig serverConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public ConfigurationTableDataProvider(
      final Injector injector,
      final ObjectMapper mapper,
      final Properties properties,
      @Self final DruidNode node,
      @Self final Set<NodeRole> nodeRoles,
      final DruidServerConfig serverConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.injector = injector;
    this.mapper = mapper;
    this.accessorMapper = mapper.copy()
                                .configure(MapperFeature.AUTO_DETECT_GETTERS, true)
                                .configure(MapperFeature.AUTO_DETECT_IS_GETTERS, true)
                                .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY)
                                .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
    this.properties = properties;
    this.node = node;
    this.nodeRoles = nodeRoles.stream().map(NodeRole::getJsonName).sorted().toList().toString();
    this.serverConfig = serverConfig;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return List.of(new SystemTablePushdownFilter("server", null), new SystemTablePushdownFilter("service_name", null));
  }

  @Override
  public Iterable<Object[]> getRows(final List<DimFilter> filters, final AuthenticationResult authenticationResult)
  {
    ConfigurationTableDescriptor.authorize(authenticationResult, authorizerMapper);
    for (final DimFilter filter : filters) {
      final String column;
      final Object value;
      if (filter instanceof SelectorDimFilter selector && selector.getExtractionFn() == null) {
        column = selector.getDimension();
        value = selector.getValue();
      } else if (filter instanceof EqualityFilter equality) {
        column = equality.getColumn();
        value = equality.getMatchValue();
      } else {
        continue;
      }
      if (("server".equals(column) && !node.getHostAndPortToUse().equals(value))
          || ("service_name".equals(column) && !node.getServiceName().equals(value))) {
        return List.of();
      }
    }

    // Inspect final bindings, not module declarations: overridden providers must not appear in the catalog.
    final Map<Key<?>, Binding<?>> bindings = new LinkedHashMap<>();
    for (Injector current = injector; current != null; current = current.getParent()) {
      current.getAllBindings().forEach(bindings::putIfAbsent);
    }
    final List<Object[]> rows = new ArrayList<>();
    for (final Binding<?> binding : bindings.values()) {
      if (binding instanceof ProviderInstanceBinding<?> providerBinding
          && providerBinding.getUserSuppliedProvider() instanceof JsonConfigProvider<?> provider) {
        final Object config = provider.getInitializedConfig();
        final Class<?> configClass = config == null ? provider.getConfigClass() : config.getClass();
        final String base = provider.getPropertyBase();
        final String prefix = base.endsWith(".") ? base.substring(0, base.length() - 1) : base;
        try {
          appendProperties(
              rows,
              prefix,
              configClass,
              binding.getKey().toString(),
              config,
              provider.getConfigClass(),
              config == null ? "NOT_INITIALIZED" : "AVAILABLE",
              new HashSet<>()
          );
        }
        catch (final JsonMappingException | RuntimeException e) {
          rows.add(row(
              prefix,
              configClass,
              binding.getKey().toString(),
              null,
              null,
              null,
              "ERROR",
              "Unable to inspect configuration"
          ));
        }
      }
    }
    rows.sort(Comparator.comparing((Object[] row) -> (String) row[3]).thenComparing(row -> (String) row[5]));
    return rows;
  }

  private void appendProperties(
      final List<Object[]> rows,
      final String prefix,
      final Class<?> configClass,
      final String binding,
      @Nullable final Object config,
      final Class<?> declaredType,
      final String valueStatus,
      final Set<Class<?>> ancestors
  ) throws JsonMappingException
  {
    final Class<?> type = config == null ? declaredType : config.getClass();
    final List<BeanPropertyDefinition> definitions = mapper.getDeserializationConfig()
        .introspect(mapper.constructType(type)).findProperties().stream()
        .filter(BeanPropertyDefinition::couldDeserialize).toList();
    if (!ancestors.add(type)) {
      rows.add(row(prefix, configClass, binding, type.getTypeName(), null, null, "UNSUPPORTED", null));
      return;
    }
    final TypeSerializer typeSerializer = mapper.getSerializerProviderInstance()
        .findTypeSerializer(mapper.constructType(declaredType));
    final boolean hasTypeProperty = typeSerializer != null
                                   && typeSerializer.getTypeIdResolver().getMechanism() == JsonTypeInfo.Id.NAME
                                   && (typeSerializer.getTypeInclusion() == JsonTypeInfo.As.PROPERTY
                                       || typeSerializer.getTypeInclusion() == JsonTypeInfo.As.EXISTING_PROPERTY);
    if (hasTypeProperty && definitions.stream().noneMatch(p -> p.getName().equals(typeSerializer.getPropertyName()))) {
      final String name = prefix + "." + typeSerializer.getPropertyName();
      final boolean hidden = serverConfig.getHiddenProperties().stream().anyMatch(
          key -> StringUtils.toLowerCase(name).contains(StringUtils.toLowerCase(key))
      );
      rows.add(row(
          name,
          configClass,
          binding,
          String.class.getName(),
          hidden ? null : properties.getProperty(name),
          hidden || config == null ? null : typeSerializer.getTypeIdResolver().idFromValue(config),
          hidden ? "REDACTED" : valueStatus,
          null
      ));
    }
    if (definitions.isEmpty() && !hasTypeProperty) {
      rows.add(row(prefix, configClass, binding, type.getTypeName(), null, null, "UNSUPPORTED", null));
    }
    // Druid's mapper disables automatic getter discovery. Enable it only for reading already known input
    // properties, so helper getters cannot introduce extra configuration paths.
    final Map<String, AnnotatedMember> getters = new LinkedHashMap<>();
    accessorMapper.getSerializationConfig().introspect(mapper.constructType(type)).findProperties().forEach(
        property -> {
          if (property.getGetter() != null) {
            getters.put(property.getInternalName(), property.getGetter());
          }
        }
    );
    for (final BeanPropertyDefinition property : definitions) {
      final String name = prefix + "." + property.getName();
      final JavaType propertyType = property.getPrimaryType();
      final Class<?> rawType = propertyType.getRawClass();
      final AnnotatedMember member = property.getPrimaryMember();
      final JsonProperty annotation = member == null ? null : member.getAnnotation(JsonProperty.class);
      final boolean hidden = serverConfig.getHiddenProperties().stream().anyMatch(
          key -> StringUtils.toLowerCase(name).contains(StringUtils.toLowerCase(key))
      ) || PasswordProvider.class.isAssignableFrom(rawType)
        || DynamicConfigProvider.class.isAssignableFrom(rawType)
        || (annotation != null && annotation.access() == JsonProperty.Access.WRITE_ONLY);
      if (hidden) {
        rows.add(row(name, configClass, binding, propertyType.toCanonical(), null, null, "REDACTED", null));
        continue;
      }

      final boolean scalar = isScalar(rawType);
      // Containers may contain secrets under arbitrary keys, and custom serializers may resolve credentials.
      if (!scalar && (propertyType.isContainerType() || rawType == Object.class || rawType.isInterface())) {
        rows.add(row(name, configClass, binding, propertyType.toCanonical(), null, null, "UNSUPPORTED", null));
        continue;
      }
      try {
        final AnnotatedMember accessor = scalar
                                         ? getters.getOrDefault(property.getInternalName(), property.getAccessor())
                                         : property.getField();
        final Object value;
        if (config == null || accessor == null) {
          value = null;
        } else {
          accessor.fixAccess(true);
          value = accessor.getValue(config);
        }
        if (scalar) {
          rows.add(row(
              name,
              configClass,
              binding,
              propertyType.toCanonical(),
              properties.getProperty(name),
              scalarValue(value),
              accessor == null ? "UNSUPPORTED" : valueStatus,
              null
          ));
        } else {
          appendProperties(
              rows,
              name,
              configClass,
              binding,
              value,
              rawType,
              accessor == null ? "UNSUPPORTED" : valueStatus,
              ancestors
          );
        }
      }
      catch (final JsonMappingException | RuntimeException e) {
        rows.add(row(
            name,
            configClass,
            binding,
            propertyType.toCanonical(),
            null,
            null,
            "ERROR",
            "Unable to read configuration property"
        ));
      }
    }
    ancestors.remove(type);
  }

  private static boolean isScalar(final Class<?> type)
  {
    return type.isPrimitive() || type == String.class || type == Boolean.class || type == Character.class
           || type == Integer.class || type == Long.class || type == Double.class || type == Float.class
           || type == Short.class || type == Byte.class || type.isEnum()
           || type == Duration.class || type == Period.class || type == HumanReadableBytes.class;
  }

  /** Converts supported value objects directly, without invoking Jackson or custom object serializers. */
  @Nullable
  private static String scalarValue(@Nullable final Object value)
  {
    if (value == null) {
      return null;
    }
    if (value instanceof HumanReadableBytes bytes) {
      return Long.toString(bytes.getBytes());
    }
    if (value instanceof Duration duration) {
      return duration.toString();
    }
    if (value instanceof Period period) {
      return period.toString();
    }
    if (value instanceof Enum<?> enumValue) {
      return enumValue.name();
    }
    return String.valueOf(value);
  }

  private Object[] row(
      final String property,
      final Class<?> configClass,
      final String binding,
      @Nullable final String type,
      @Nullable final String configured,
      @Nullable final String effective,
      final String status,
      @Nullable final String error
  )
  {
    return new Object[]{
        node.getHostAndPortToUse(), node.getServiceName(), nodeRoles, property, configClass.getName(), binding,
        type, configured, effective, status, error
    };
  }
}
