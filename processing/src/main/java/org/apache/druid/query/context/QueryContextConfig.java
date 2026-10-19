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

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Runtime configuration for user-supplied query contexts, bound to the {@code druid.query.context} property prefix.
 */
public class QueryContextConfig
{
  public static final QueryContextConfig DEFAULT = new QueryContextConfig(null);

  /**
   * Whether the Broker rejects queries whose context contains a parameter that is not declared in
   * {@link QueryContextParameters}.
   */
  @JsonProperty
  private final boolean rejectUnknownParameters;

  @JsonCreator
  public QueryContextConfig(@JsonProperty("rejectUnknownParameters") @Nullable final Boolean rejectUnknownParameters)
  {
    this.rejectUnknownParameters = rejectUnknownParameters == null || rejectUnknownParameters;
  }

  public boolean isRejectUnknownParameters()
  {
    return rejectUnknownParameters;
  }

  /**
   * Validates the names of the parameters a user set in a query context, if {@link #isRejectUnknownParameters()}.
   *
   * @throws org.apache.druid.query.BadQueryContextException for the alphabetically first name that is not declared
   */
  public void validateParameterNames(final Collection<String> names)
  {
    if (rejectUnknownParameters) {
      QueryContextParameterValidator.rejectUnknownParameters(names);
    }
  }
}
