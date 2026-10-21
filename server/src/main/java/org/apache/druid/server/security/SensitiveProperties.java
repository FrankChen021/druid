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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.log.StartupLoggingConfig;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Decides which runtime properties are sensitive, so that their values are not exposed by the startup property
 * logging, the {@code /status/properties} endpoint, or the logged task commands.
 *
 * Bound to the {@code druid.server} prefix. A property is sensitive if its name contains, ignoring case, any word in
 * {@code druid.server.sensitiveProperties}. For backward compatibility, the words in the deprecated
 * {@code druid.server.hiddenProperties} and {@code druid.startup.logging.maskProperties} are honored as well.
 */
public class SensitiveProperties
{
  private static final Logger log = new Logger(SensitiveProperties.class);

  public static final Set<String> DEFAULT_SENSITIVE_PROPERTIES = ImmutableSet.of(
      "password",
      "passphrase",
      "secret",
      "key",
      "token",
      "pwd",
      "basicAuthentication",
      "druid.emitter.kafka.producer.hiddenProperties"
  );

  @JsonProperty
  private final Set<String> sensitiveProperties;

  private final Set<String> lowerCaseWords = new HashSet<>();

  /**
   * @param sensitiveProperties  value of {@code druid.server.sensitiveProperties}, defaults to
   *                             {@link #DEFAULT_SENSITIVE_PROPERTIES}
   * @param druidServerConfig    provides the deprecated {@code druid.server.hiddenProperties}
   * @param startupLoggingConfig provides the deprecated {@code druid.startup.logging.maskProperties}
   */
  @JsonCreator
  @SuppressWarnings("removal") // reads the deprecated hiddenProperties and maskProperties for backward compatibility
  public SensitiveProperties(
      @JsonProperty("sensitiveProperties") @Nullable final Set<String> sensitiveProperties,
      @JacksonInject @Nullable final DruidServerConfig druidServerConfig,
      @JacksonInject @Nullable final StartupLoggingConfig startupLoggingConfig
  )
  {
    this.sensitiveProperties = sensitiveProperties == null ? DEFAULT_SENSITIVE_PROPERTIES : sensitiveProperties;

    addWords(this.sensitiveProperties);
    if (druidServerConfig != null && druidServerConfig.getHiddenProperties() != null) {
      log.warn(
          "Property [druid.server.hiddenProperties] is deprecated and will be removed in a future release;"
          + " its values %s are still applied. Move them to [druid.server.sensitiveProperties].",
          druidServerConfig.getHiddenProperties()
      );
      addWords(druidServerConfig.getHiddenProperties());
    }
    if (startupLoggingConfig != null && startupLoggingConfig.getMaskProperties() != null) {
      log.warn(
          "Property [druid.startup.logging.maskProperties] is deprecated and will be removed in a future release;"
          + " its values %s are still applied. Move them to [druid.server.sensitiveProperties].",
          startupLoggingConfig.getMaskProperties()
      );
      addWords(startupLoggingConfig.getMaskProperties());
    }
  }

  private void addWords(final Collection<String> words)
  {
    words.forEach(word -> lowerCaseWords.add(StringUtils.toLowerCase(word)));
  }

  /**
   * Returns true if the property name contains any of the configured words, ignoring case.
   */
  public boolean isSensitive(final String propertyName)
  {
    final String lowerCaseName = StringUtils.toLowerCase(propertyName);
    return lowerCaseWords.stream().anyMatch(lowerCaseName::contains);
  }
}
