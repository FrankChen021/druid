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

package org.apache.druid.query.memory;

import org.apache.druid.query.ResourceLimitExceededException;

import java.util.Objects;

/** Structured resource-limit failure raised by the query-memory manager. */
public class QueryMemoryException extends ResourceLimitExceededException
{
  public enum Reason
  {
    NODE_LIMIT,
    QUERY_LIMIT,
    TIMEOUT,
    INTERRUPTED,
    CANCELED,
    INVALID_REQUEST
  }

  private final Reason reason;

  public QueryMemoryException(final Reason reason, final String message)
  {
    super(message);
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  public Reason getReason()
  {
    return reason;
  }
}
