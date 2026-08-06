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

import java.util.Arrays;

public class JUnit5Matchers
{
  interface TestMatcher
  {
    boolean matches(Object actual);
  }

  public static class CombinableMatcher implements TestMatcher
  {
    private final TestMatcher delegate;

    private CombinableMatcher(final TestMatcher delegate)
    {
      this.delegate = delegate;
    }

    public TestMatcher and(final Object other)
    {
      return actual -> delegate.matches(actual) && JUnit5Assertions.matches(actual, other);
    }

    @Override
    public boolean matches(final Object actual)
    {
      return delegate.matches(actual);
    }
  }

  private JUnit5Matchers()
  {
  }

  public static TestMatcher instanceOf(final Class<?> clazz)
  {
    return clazz::isInstance;
  }

  public static TestMatcher startsWith(final String prefix)
  {
    return actual -> actual instanceof String && ((String) actual).startsWith(prefix);
  }

  public static TestMatcher containsString(final String substring)
  {
    return actual -> actual instanceof String && ((String) actual).contains(substring);
  }

  public static TestMatcher contains(final Object expected)
  {
    return actual -> actual instanceof Iterable && contains((Iterable<?>) actual, expected);
  }

  public static TestMatcher not(final Object matcher)
  {
    return actual -> !JUnit5Assertions.matches(actual, matcher);
  }

  public static TestMatcher anyOf(final Object... matchers)
  {
    return actual -> Arrays.stream(matchers).anyMatch(matcher -> JUnit5Assertions.matches(actual, matcher));
  }

  public static TestMatcher is(final Object matcherOrValue)
  {
    if (matcherOrValue instanceof TestMatcher) {
      return (TestMatcher) matcherOrValue;
    }
    return actual -> java.util.Objects.equals(actual, matcherOrValue);
  }

  public static CombinableMatcher both(final Object matcher)
  {
    return new CombinableMatcher(actual -> JUnit5Assertions.matches(actual, matcher));
  }

  public static TestMatcher greaterThanOrEqualTo(final Comparable<?> expected)
  {
    return actual -> compare(actual, expected) >= 0;
  }

  public static TestMatcher lessThanOrEqualTo(final Comparable<?> expected)
  {
    return actual -> compare(actual, expected) <= 0;
  }

  public static TestMatcher lessThan(final Comparable<?> expected)
  {
    return actual -> compare(actual, expected) < 0;
  }

  public static TestMatcher closeTo(final double expected, final double error)
  {
    return actual -> actual instanceof Number && Math.abs(((Number) actual).doubleValue() - expected) <= error;
  }

  public static TestMatcher hasMessage(final Object matcher)
  {
    return actual -> actual instanceof Throwable
                     && JUnit5Assertions.matches(((Throwable) actual).getMessage(), matcher);
  }

  @SuppressWarnings("unchecked")
  private static int compare(final Object actual, final Comparable<?> expected)
  {
    return ((Comparable<Object>) actual).compareTo(expected);
  }

  private static boolean contains(final Iterable<?> values, final Object expected)
  {
    for (final Object value : values) {
      if (java.util.Objects.equals(value, expected)) {
        return true;
      }
    }
    return false;
  }
}
