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

package org.apache.druid.testing.junit5;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JUnit5Assertions extends org.junit.jupiter.api.Assertions
{
  private JUnit5Assertions()
  {
  }

  public static void assertMatches(final Object actual, final Object matcher)
  {
    assertMatches(null, actual, matcher);
  }

  public static void assertMatches(final String reason, final Object actual, final Object matcher)
  {
    if (!matches(actual, matcher)) {
      final String prefix = reason == null ? "" : reason + ": ";
      throw new AssertionError(prefix + "value [" + actual + "] did not match [" + matcher + "]");
    }
  }

  static boolean matches(final Object actual, final Object matcher)
  {
    if (matcher instanceof JUnit5Matchers.TestMatcher) {
      return ((JUnit5Matchers.TestMatcher) matcher).matches(actual);
    }
    try {
      final Method method = matcher.getClass().getMethod("matches", Object.class);
      return (boolean) method.invoke(matcher, actual);
    }
    catch (NoSuchMethodException | IllegalAccessException e) {
      throw new IllegalArgumentException("Unsupported matcher " + matcher, e);
    }
    catch (InvocationTargetException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
