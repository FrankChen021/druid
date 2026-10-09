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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Discovers installed JsonConfigProvider bindings without provisioning them. Scalar values are read from the
 * already initialized objects, including collections of scalars; arbitrary object serializers and secret providers
 * are never invoked.
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
              config,
              provider.getConfigClass(),
              config == null ? "NOT_INITIALIZED" : "AVAILABLE",
              new HashSet<>()
          );
        }
        catch (final JsonProcessingException | RuntimeException e) {
          rows.add(row(
              prefix,
              configClass,
              null,
              null,
              null,
              "ERROR",
              "Unable to inspect configuration"
          ));
        }
      }
    }
    final Map<String, Object[]> propertiesByPath = new LinkedHashMap<>();
    for (final Object[] row : rows) {
      final Object[] previous = propertiesByPath.putIfAbsent((String) row[3], row);
      if (previous == null) {
        continue;
      }
      if (!Objects.equals(previous[4], row[4])) {
        previous[4] = null;
      }
      if (!Arrays.equals(previous, 5, 10, row, 5, 10)) {
        previous[5] = Objects.equals(previous[5], row[5]) ? previous[5] : null;
        previous[6] = null;
        previous[7] = null;
        previous[8] = "CONFLICT";
        previous[9] = "Configuration consumers disagree on type, value, or inspection status";
      }
    }
    final List<Object[]> uniqueRows = new ArrayList<>(propertiesByPath.values());
    uniqueRows.sort(Comparator.comparing(row -> (String) row[3]));
    return uniqueRows;
  }

  private void appendProperties(
      final List<Object[]> rows,
      final String prefix,
      final Class<?> configClass,
      @Nullable final Object config,
      final Class<?> declaredType,
      final String valueStatus,
      final Set<Class<?>> ancestors
  ) throws JsonProcessingException
  {
    final Class<?> type = config == null ? declaredType : config.getClass();
    final List<BeanPropertyDefinition> definitions = mapper.getDeserializationConfig()
        .introspect(mapper.constructType(type)).findProperties().stream()
        .filter(BeanPropertyDefinition::couldDeserialize).toList();
    if (!ancestors.add(type)) {
      rows.add(row(prefix, configClass, type.getTypeName(), null, null, "UNSUPPORTED", null));
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
          String.class.getName(),
          hidden ? null : properties.getProperty(name),
          hidden || config == null ? null : typeSerializer.getTypeIdResolver().idFromValue(config),
          hidden ? "REDACTED" : valueStatus,
          null
      ));
    }
    if (definitions.isEmpty() && !hasTypeProperty) {
      rows.add(row(prefix, configClass, type.getTypeName(), null, null, "UNSUPPORTED", null));
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
        rows.add(row(name, configClass, propertyType.toCanonical(), null, null, "REDACTED", null));
        continue;
      }

      final boolean scalar = isScalar(rawType);
      final boolean scalarCollection = (rawType.isArray() || Collection.class.isAssignableFrom(rawType))
                                       && propertyType.getContentType() != null
                                       && isScalar(propertyType.getContentType().getRawClass());
      // Maps may contain secrets under arbitrary keys, and custom serializers may resolve credentials.
      if (!scalar && !scalarCollection
          && (propertyType.isContainerType() || rawType == Object.class || rawType.isInterface())) {
        rows.add(row(name, configClass, propertyType.toCanonical(), null, null, "UNSUPPORTED", null));
        continue;
      }
      try {
        final AnnotatedMember accessor = scalar || scalarCollection
                                         ? getters.getOrDefault(property.getInternalName(), property.getAccessor())
                                         : property.getField();
        final Object value;
        if (config == null || accessor == null) {
          value = null;
        } else {
          accessor.fixAccess(true);
          value = accessor.getValue(config);
        }
        if (scalar || scalarCollection) {
          rows.add(row(
              name,
              configClass,
              propertyType.toCanonical(),
              properties.getProperty(name),
              effectiveValue(value, propertyType),
              accessor == null ? "UNSUPPORTED" : valueStatus,
              null
          ));
        } else {
          appendProperties(
              rows,
              name,
              configClass,
              value,
              rawType,
              accessor == null ? "UNSUPPORTED" : valueStatus,
              ancestors
          );
        }
      }
      catch (final JsonProcessingException | RuntimeException e) {
        rows.add(row(
            name,
            configClass,
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

  @Nullable
  private String effectiveValue(@Nullable final Object value, final JavaType type) throws JsonProcessingException
  {
    if (value == null || isScalar(type.getRawClass())) {
      final Object scalar = scalarValue(value, type.getRawClass());
      return scalar == null ? null : scalar.toString();
    }
    // Copy only supported scalar elements, so custom objects never reach JSON serialization.
    final List<Object> elements = new ArrayList<>();
    final Class<?> elementType = type.getContentType().getRawClass();
    if (value instanceof Collection<?> collection) {
      for (final Object element : collection) {
        elements.add(scalarValue(element, elementType));
      }
    } else {
      for (int i = 0; i < Array.getLength(value); i++) {
        elements.add(scalarValue(Array.get(value, i), elementType));
      }
    }
    return mapper.writeValueAsString(elements);
  }

  /** Converts supported value objects directly, without invoking Jackson or custom object serializers. */
  @Nullable
  private static Object scalarValue(@Nullable final Object value, final Class<?> declaredType)
  {
    if (value == null) {
      return null;
    }
    if (!isScalar(value.getClass()) && !(value instanceof Enum<?>)) {
      throw new IllegalArgumentException("Unsupported scalar configuration value");
    }
    if (declaredType == HumanReadableBytes.class) {
      final long bytes = value instanceof HumanReadableBytes readable ? readable.getBytes() : ((Number) value).longValue();
      return HumanReadableBytes.format(bytes, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE);
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
    return value instanceof Character ? value.toString() : value;
  }

  private Object[] row(
      final String property,
      final Class<?> configClass,
      @Nullable final String type,
      @Nullable final String configured,
      @Nullable final String effective,
      final String status,
      @Nullable final String error
  )
  {
    return new Object[]{
        node.getHostAndPortToUse(), node.getServiceName(), nodeRoles, property, configClass.getName(),
        type, configured, effective, status, error
    };
  }
}
