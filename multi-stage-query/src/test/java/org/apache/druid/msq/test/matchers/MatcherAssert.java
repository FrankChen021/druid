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

package org.apache.druid.msq.test.matchers;

import org.junit.jupiter.api.Assertions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MatcherAssert
{
  private MatcherAssert()
  {
  }

  public static <T> void assertThat(final T actual, final Matcher<? super T> matcher)
  {
    Assertions.assertTrue(matcher.matches(actual), matcher.describe());
  }

  public static <T> void assertThat(final String reason, final T actual, final Matcher<? super T> matcher)
  {
    Assertions.assertTrue(matcher.matches(actual), reason + ": " + matcher.describe());
  }

  public static <T> void assertThat(final T actual, final Object matcher)
  {
    assertMatches(actual, matcher);
  }

  public static <T> void assertThat(final String reason, final T actual, final Object matcher)
  {
    try {
      assertMatches(actual, matcher);
    }
    catch (AssertionError e) {
      throw new AssertionError(reason + ": " + e.getMessage(), e);
    }
  }

  private static <T> void assertMatches(final T actual, final Object assertion)
  {
    try {
      for (Method method : assertion.getClass().getMethods()) {
        if (method.getName().equals("assertMatches")
            && method.getParameterCount() == 1
            && method.getParameterTypes()[0].isInstance(actual)) {
          method.invoke(assertion, actual);
          return;
        }
      }

      final boolean matches = (boolean) assertion.getClass().getMethod("matches", Object.class)
                                                    .invoke(assertion, actual);
      Assertions.assertTrue(matches, assertion::toString);
    }
    catch (InvocationTargetException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalArgumentException("Assertion invocation failed", cause);
    }
    catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Unsupported assertion type: " + assertion.getClass().getName(), e);
    }
  }
}
