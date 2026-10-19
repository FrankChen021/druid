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

package org.apache.druid.query.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BadQueryContextException;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Runtime configuration for validating query contexts, bound to the {@code druid.query.context} property prefix.
 */
public class QueryContextConfig
{
  public static final QueryContextConfig DEFAULT = new QueryContextConfig(null);

  /**
   * Whether the Broker rejects queries whose context contains a parameter that is not declared in
   * {@link QueryContextParameters}. Off by default, so that queries with such parameters keep working.
   */
  @JsonProperty
  private final boolean rejectUnknownParameters;

  @JsonCreator
  public QueryContextConfig(@JsonProperty("rejectUnknownParameters") @Nullable final Boolean rejectUnknownParameters)
  {
    this.rejectUnknownParameters = rejectUnknownParameters != null && rejectUnknownParameters;
  }

  public boolean isRejectUnknownParameters()
  {
    return rejectUnknownParameters;
  }

  /**
   * Validates the context that a user set on a query. The values of declared parameters are always validated, and
   * unknown parameters are rejected if {@link #isRejectUnknownParameters()}.
   *
   * @throws BadQueryContextException for the first invalid parameter, as found by
   *                                  {@link QueryContextParameterValidator#validate}
   */
  public void validate(final Map<String, Object> context)
  {
    QueryContextParameterValidator.validate(context, rejectUnknownParameters);
  }

  /**
   * Validates a default context that operators configure, such as {@code druid.query.default.context}, if
   * {@link #isRejectUnknownParameters()}. Defaults are merged into every query, so they are validated where they are
   * configured rather than with each query, and an invalid default is reported as a configuration error.
   *
   * @param source where the default context is configured, such as a property prefix, for the error message
   *
   * @throws IAE for the first invalid parameter
   */
  public void validateDefaults(final String source, final Map<String, Object> defaults)
  {
    if (!rejectUnknownParameters) {
      return;
    }
    try {
      QueryContextParameterValidator.validate(defaults, true);
    }
    catch (BadQueryContextException e) {
      throw new IAE("Invalid default query context [%s]: %s", source, e.getMessage());
    }
  }
}
