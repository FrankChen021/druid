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

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Validates query context parameter names and values against the descriptor catalog. */
public final class QueryContextParameterValidator
{
  /**
   * Maximum number of similar parameter names suggested for an unknown parameter.
   */
  private static final int MAX_HINTS = 3;

  private QueryContextParameterValidator()
  {
  }

  /**
   * Validates a value assigned by a SQL {@code SET} statement.
   *
   * @throws BadQueryContextException if the parameter is recognized and the value is invalid
   */
  public static void validate(final String name, @Nullable final Object value)
  {
    final QueryContextParameter<?> parameter = QueryContextParameters.ALL.get().get(name);
    // Unmigrated parameters are intentionally accepted until the catalog contains every supported context parameter.
    if (parameter != null) {
      parameter.parse(value);
    }
  }

  /**
   * Validates every recognized query context parameter in the supplied map.
   *
   * @throws BadQueryContextException if a recognized parameter has an invalid value
   */
  public static void validate(final Map<String, Object> parameters)
  {
    parameters.forEach(QueryContextParameterValidator::validate);
  }

  /**
   * Rejects the alphabetically first name that is not a declared query context parameter. Names are checked in sorted
   * order so that the same context is reported the same way however it reached Druid, since some paths, such as
   * {@link org.apache.druid.query.QueryContext}, sort the context. The error suggests up to {@value #MAX_HINTS}
   * declared parameters with similar names, as found by {@link #getHints}.
   *
   * @throws BadQueryContextException if a name is not a declared parameter
   */
  public static void rejectUnknownParameters(final Collection<String> names)
  {
    final Map<String, QueryContextParameter<?>> parameters = QueryContextParameters.ALL.get();
    final Optional<String> unknownName =
        names.stream().filter(name -> !parameters.containsKey(name)).min(Comparator.naturalOrder());
    if (unknownName.isPresent()) {
      final String name = unknownName.get();
      final List<String> hints = getHints(name, parameters.values());
      if (hints.isEmpty()) {
        throw new BadQueryContextException(StringUtils.format("Unknown query context parameter [%s].", name));
      }
      throw new BadQueryContextException(
          StringUtils.format(
              "Unknown query context parameter [%s]. Did you mean [%s]?",
              name,
              String.join(", ", hints)
          )
      );
    }
  }

  /**
   * Returns the names of up to {@value #MAX_HINTS} user-settable, non-deprecated parameters that are similar to
   * {@code name}, closest first. Follows the typo correction of ClickHouse's {@code NamePrompter}, which comes from
   * Clang's {@code SimpleTypoCorrector}: a name is similar when its case-insensitive Levenshtein distance from
   * {@code name} is at most one edit per three characters of {@code name}.
   */
  static List<String> getHints(final String name, final Collection<QueryContextParameter<?>> candidates)
  {
    final int mistakeFactor = (name.length() + 2) / 3;
    final LevenshteinDistance levenshteinDistance = new LevenshteinDistance(mistakeFactor);
    final String lowerCaseName = StringUtils.toLowerCase(name);

    final List<Hint> hints = new ArrayList<>();
    for (final QueryContextParameter<?> candidate : candidates) {
      if (candidate.getVisibility() == QueryContextParameter.Visibility.SYSTEM || candidate.isDeprecated()) {
        continue;
      }

      final String candidateName = candidate.getName();
      // The length difference is a lower bound of the distance, so skip candidates that cannot be close enough.
      final int lengthDifference = Math.abs(name.length() - candidateName.length());
      if (lengthDifference > mistakeFactor || (lengthDifference > 0 && name.length() / lengthDifference < 3)) {
        continue;
      }

      // The distance is -1 when it exceeds the mistake factor.
      final int distance = levenshteinDistance.apply(lowerCaseName, StringUtils.toLowerCase(candidateName));
      if (distance >= 0) {
        hints.add(new Hint(candidateName, distance));
      }
    }

    return hints.stream()
                .sorted(Comparator.comparingInt(Hint::distance).thenComparing(Hint::name))
                .limit(MAX_HINTS)
                .map(Hint::name)
                .toList();
  }

  private record Hint(String name, int distance)
  {
  }
}
