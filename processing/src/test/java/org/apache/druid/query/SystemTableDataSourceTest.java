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


package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class SystemTableDataSourceTest
{
  private final SystemTableDataSource dataSource = new SystemTableDataSource("server_properties");

  /** The table is namespaced so authorization and cancellation cannot collide with a regular datasource. */
  @Test
  public void testGetTableNames()
  {
    Assertions.assertEquals(Collections.singleton("sys.server_properties"), dataSource.getTableNames());
  }

  /** The datasource is resolved by its owning service, so it must never be treated as cacheable segment data. */
  @Test
  public void testIsNotCacheableGlobalOrProcessable()
  {
    Assertions.assertTrue(dataSource.getChildren().isEmpty());
    Assertions.assertFalse(dataSource.isCacheable(true));
    Assertions.assertFalse(dataSource.isGlobal());
    Assertions.assertFalse(dataSource.isProcessable());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final String json = jsonMapper.writeValueAsString(dataSource);

    Assertions.assertEquals("{\"type\":\"systemTable\",\"table\":\"server_properties\"}", json);
    Assertions.assertEquals(dataSource, jsonMapper.readValue(json, DataSource.class));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SystemTableDataSource.class).usingGetClass().withNonnullFields("table").verify();
  }
}
