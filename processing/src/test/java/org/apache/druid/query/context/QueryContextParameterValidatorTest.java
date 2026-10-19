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

import org.apache.druid.query.BadQueryContextException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryContextParameterValidatorTest
{
  @Test
  void testValidateParameter()
  {
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", -1));
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", "not-an-int"));
    assertThrows(BadQueryContextException.class, () -> QueryContextParameterValidator.validate("useResultLevelCache", 1));
    QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", Integer.MAX_VALUE);
    QueryContextParameterValidator.validate("maxRowsQueuedForOrdering", null);
    QueryContextParameterValidator.validate("unmigratedParameter", -1);
  }

  @Test
  void testValidateParameters()
  {
    assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.validate(
            Map.of("maxRowsQueuedForOrdering", 0, "unmigratedParameter", -1)
        )
    );
    QueryContextParameterValidator.validate(Map.of("maxRowsQueuedForOrdering", 1, "unmigratedParameter", -1));
  }

  @Test
  void testRejectUnknownParametersAcceptsDeclaredParameters()
  {
    QueryContextParameterValidator.rejectUnknownParameters(List.of());
    QueryContextParameterValidator.rejectUnknownParameters(List.of("useCache", "timeout", "sqlQueryId", "queryFailTime"));
  }

  @Test
  void testRejectUnknownParameterWithHint()
  {
    final BadQueryContextException e = assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.rejectUnknownParameters(List.of("timeout", "useCahce"))
    );
    assertEquals("Unknown query context parameter [useCahce]. Did you mean [useCache]?", e.getMessage());
  }

  @Test
  void testRejectUnknownParameterWithoutHint()
  {
    final BadQueryContextException e = assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.rejectUnknownParameters(List.of("myTraceId"))
    );
    assertEquals("Unknown query context parameter [myTraceId].", e.getMessage());
  }

  @Test
  void testRejectUnknownParametersReportsAlphabeticallyFirstUnknownParameter()
  {
    final BadQueryContextException e = assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.rejectUnknownParameters(List.of("useCahce", "useCache", "myTraceId"))
    );
    assertEquals("Unknown query context parameter [myTraceId].", e.getMessage());
  }

  @Test
  void testParameterNamesAreCaseSensitive()
  {
    final BadQueryContextException e = assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.rejectUnknownParameters(List.of("usecache"))
    );
    assertEquals("Unknown query context parameter [usecache]. Did you mean [useCache]?", e.getMessage());
  }

  @Test
  void testGetHints()
  {
    final List<QueryContextParameter<?>> candidates = List.of(
        parameter("cardinalities").build(),
        parameter("cardinality").build(),
        parameter("cardinalityLimit").build(),
        parameter("carinality").build(),
        parameter("cardinalyti").build()
    );

    // Closest first, then by name, at most three.
    assertEquals(
        List.of("cardinality", "cardinalyti", "carinality"),
        QueryContextParameterValidator.getHints("cardinalty", candidates)
    );
    // Case-insensitive.
    assertEquals(
        List.of("cardinality", "carinality", "cardinalyti"),
        QueryContextParameterValidator.getHints("CARDINALITY", candidates)
    );
    // At most one edit per three characters: "cxrdxnxlxtx" is five substitutions away from "cardinality".
    assertEquals(List.of(), QueryContextParameterValidator.getHints("cxrdxnxlxtx", candidates));
    // Three insertions are within the budget of "cardinal", but the names differ in length by more than a third.
    assertEquals(List.of(), QueryContextParameterValidator.getHints("cardinal", candidates));
  }

  @Test
  void testGetHintsSkipsSystemAndDeprecatedParameters()
  {
    final List<QueryContextParameter<?>> candidates = List.of(
        parameter("systemParameter").visibility(QueryContextParameter.Visibility.SYSTEM).build(),
        parameter("deprecatedParameter").deprecated("Use another parameter.").build()
    );

    assertEquals(List.of(), QueryContextParameterValidator.getHints("systemParameters", candidates));
    assertEquals(List.of(), QueryContextParameterValidator.getHints("deprecatedParameters", candidates));
  }

  private static QueryContextParameter.Builder<String> parameter(final String name)
  {
    return QueryContextParameters.stringParameter(name).description("Test parameter.");
  }
}
