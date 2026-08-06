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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

/** Jupiter equivalent for tests that declare an expected failure before invoking the subject. */
public class ExpectedFailureExtension
    implements BeforeEachCallback, AfterEachCallback, TestExecutionExceptionHandler
{
  private Class<? extends Throwable> expectedType;
  private Class<? extends Throwable> expectedCauseType;
  private String expectedMessage;
  private boolean messageMustStartWith;
  private boolean handled;

  public static ExpectedFailureExtension none()
  {
    return new ExpectedFailureExtension();
  }

  public void expect(final Class<? extends Throwable> type)
  {
    expectedType = type;
  }

  public void expectMessage(final String message)
  {
    expectedMessage = message;
  }

  public void expectMessageStartsWith(final String message)
  {
    expectedMessage = message;
    messageMustStartWith = true;
  }

  public void expectCause(final Class<? extends Throwable> causeType)
  {
    expectedCauseType = causeType;
  }

  @Override
  public void beforeEach(final ExtensionContext context)
  {
    reset();
  }

  @Override
  public void handleTestExecutionException(final ExtensionContext context, final Throwable throwable) throws Throwable
  {
    if (expectedType == null) {
      throw throwable;
    }
    if (!expectedType.isInstance(throwable)) {
      throw new AssertionError("Expected " + expectedType.getName() + " but caught " + throwable, throwable);
    }
    if (expectedMessage != null && !messageMatches(throwable.getMessage())) {
      throw new AssertionError("Expected message containing [" + expectedMessage + "] but was [" + throwable.getMessage() + "]", throwable);
    }
    if (expectedCauseType != null && !expectedCauseType.isInstance(throwable.getCause())) {
      throw new AssertionError("Expected cause " + expectedCauseType.getName() + " but was " + throwable.getCause(), throwable);
    }
    handled = true;
  }

  @Override
  public void afterEach(final ExtensionContext context)
  {
    if (expectedType != null && !handled) {
      final Class<? extends Throwable> missingType = expectedType;
      reset();
      throw new AssertionError("Expected test to throw " + missingType.getName());
    }
    reset();
  }

  private void reset()
  {
    expectedType = null;
    expectedCauseType = null;
    expectedMessage = null;
    messageMustStartWith = false;
    handled = false;
  }

  private boolean messageMatches(final String actualMessage)
  {
    return actualMessage != null
           && (messageMustStartWith ? actualMessage.startsWith(expectedMessage) : actualMessage.contains(expectedMessage));
  }
}
