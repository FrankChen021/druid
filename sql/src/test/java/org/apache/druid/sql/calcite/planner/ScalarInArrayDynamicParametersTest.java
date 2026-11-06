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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ScalarInArrayDynamicParametersTest
{
  @Test
  public void testAddAssignsIndexesFromFirstAvailableIndex()
  {
    final ScalarInArrayDynamicParameters parameters = new ScalarInArrayDynamicParameters(() -> 3);
    final List<Object> first = Arrays.asList("a", "b");
    final List<Object> second = Arrays.asList(1L, 2L);

    Assertions.assertEquals(3, parameters.add(first));
    Assertions.assertEquals(4, parameters.add(second));
    Assertions.assertEquals(first, parameters.get(3));
    Assertions.assertEquals(second, parameters.get(4));
    Assertions.assertNull(parameters.get(0));
    Assertions.assertNull(parameters.get(5));
  }

  @Test
  public void testIdenticalValuesShareIndex()
  {
    final ScalarInArrayDynamicParameters parameters = new ScalarInArrayDynamicParameters(() -> 0);

    Assertions.assertEquals(0, parameters.add(Arrays.asList("a", "b")));
    Assertions.assertEquals(0, parameters.add(Arrays.asList("a", "b")));
    Assertions.assertEquals(1, parameters.add(Arrays.asList("b", "a")));
  }

  @Test
  public void testValuesOfDifferentTypesDoNotShareIndex()
  {
    final ScalarInArrayDynamicParameters parameters = new ScalarInArrayDynamicParameters(() -> 0);

    Assertions.assertEquals(0, parameters.add(Arrays.asList(1L, 2L)));
    Assertions.assertEquals(1, parameters.add(Arrays.asList(1.0, 2.0)));
    Assertions.assertEquals(2, parameters.add(Arrays.asList("1", "2")));
  }

  @Test
  public void testFirstIndexSupplierCalledOnce()
  {
    final AtomicInteger calls = new AtomicInteger();
    final ScalarInArrayDynamicParameters parameters = new ScalarInArrayDynamicParameters(() -> {
      calls.incrementAndGet();
      return 0;
    });

    Assertions.assertNull(parameters.get(0));
    Assertions.assertEquals(0, calls.get());
    parameters.add(Arrays.asList("a"));
    parameters.add(Arrays.asList("b"));
    Assertions.assertEquals(1, calls.get());
  }

  @Test
  public void testFirstAvailableIndex() throws SqlParseException
  {
    final SqlNode withParams = SqlParser.create("SELECT * FROM t WHERE a = ? AND b IN (?, ?)").parseQuery();
    final SqlNode withoutParams = SqlParser.create("SELECT * FROM t WHERE a = 1").parseQuery();

    // Parameters left in the statement take precedence over fewer bound values, such as during PREPARE.
    Assertions.assertEquals(3, ScalarInArrayDynamicParameters.firstAvailableIndex(withParams, 0));
    Assertions.assertEquals(5, ScalarInArrayDynamicParameters.firstAvailableIndex(withParams, 5));
    Assertions.assertEquals(0, ScalarInArrayDynamicParameters.firstAvailableIndex(withoutParams, 0));
    Assertions.assertEquals(2, ScalarInArrayDynamicParameters.firstAvailableIndex(withoutParams, 2));
    Assertions.assertEquals(1, ScalarInArrayDynamicParameters.firstAvailableIndex(null, 1));
  }
}
