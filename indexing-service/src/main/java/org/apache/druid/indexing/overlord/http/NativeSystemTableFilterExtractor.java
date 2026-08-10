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

package org.apache.druid.indexing.overlord.http;

import org.apache.druid.query.Query;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Extracts safe top-level native-filter conjuncts without changing the query's residual filter. */
public final class NativeSystemTableFilterExtractor
{
  private NativeSystemTableFilterExtractor()
  {
  }

  public static List<DimFilter> extract(
      final Query<?> query,
      final List<NativeSystemTableFilterRule> rules
  )
  {
    if (rules.isEmpty()) {
      return Collections.emptyList();
    }

    final List<DimFilter> extracted = new ArrayList<>();
    extractConjuncts(query.getFilter(), rules, extracted);
    if (query instanceof WindowOperatorQuery) {
      for (final OperatorFactory leafOperator : ((WindowOperatorQuery) query).getLeafOperators()) {
        if (leafOperator instanceof ScanOperatorFactory) {
          extractConjuncts(((ScanOperatorFactory) leafOperator).getFilter(), rules, extracted);
        }
      }
    }
    return Collections.unmodifiableList(extracted);
  }

  private static void extractConjuncts(
      @Nullable final DimFilter filter,
      final List<NativeSystemTableFilterRule> rules,
      final List<DimFilter> extracted
  )
  {
    if (filter instanceof AndDimFilter) {
      for (final DimFilter field : ((AndDimFilter) filter).getFields()) {
        extractConjuncts(field, rules, extracted);
      }
    } else if (filter != null && rules.stream().anyMatch(rule -> rule.matches(filter))) {
      extracted.add(filter);
    }
  }
}
