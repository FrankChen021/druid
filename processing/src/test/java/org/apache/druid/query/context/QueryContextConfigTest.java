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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BadQueryContextException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryContextConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  void testSerde() throws Exception
  {
    assertFalse(JSON_MAPPER.readValue("{}", QueryContextConfig.class).isRejectUnknownParameters());
    assertTrue(
        JSON_MAPPER.readValue("{\"rejectUnknownParameters\": true}", QueryContextConfig.class)
                   .isRejectUnknownParameters()
    );
    assertFalse(
        JSON_MAPPER.readValue("{\"rejectUnknownParameters\": false}", QueryContextConfig.class)
                   .isRejectUnknownParameters()
    );
  }

  @Test
  void testDefault()
  {
    assertFalse(QueryContextConfig.DEFAULT.isRejectUnknownParameters());
    assertTrue(new QueryContextConfig(true).isRejectUnknownParameters());
  }

  @Test
  void testValidate()
  {
    final Map<String, Object> unknownParameter = Map.of("useCahce", false);
    QueryContextConfig.DEFAULT.validate(unknownParameter);
    assertThrows(BadQueryContextException.class, () -> new QueryContextConfig(true).validate(unknownParameter));

    // Values are validated whether or not unknown parameters are rejected.
    assertThrows(
        BadQueryContextException.class,
        () -> QueryContextConfig.DEFAULT.validate(Map.of("maxRowsQueuedForOrdering", 0))
    );
  }

  @Test
  void testValidateDefaults()
  {
    // Neither names nor values are validated unless unknown parameters are rejected.
    QueryContextConfig.DEFAULT.validateDefaults("druid.query.default.context", Map.of("useCahce", false));
    QueryContextConfig.DEFAULT.validateDefaults("druid.query.default.context", Map.of("maxRowsQueuedForOrdering", 0));

    final IAE e = assertThrows(
        IAE.class,
        () -> new QueryContextConfig(true).validateDefaults("druid.query.default.context", Map.of("useCahce", false))
    );
    assertEquals(
        "Invalid default query context [druid.query.default.context]: "
        + "Unknown query context parameter [useCahce]. Did you mean [useCache]?",
        e.getMessage()
    );
  }
}
