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

package org.apache.druid.query.groupby;

import org.apache.druid.query.QueryResourceId;

/**
 * Prepares the resources required by a GroupBy query.
 *
 * <p>The implementation determines how resources are backed. The legacy implementation reserves buffers from the
 * process-wide blocking pool, while the query-memory implementation reserves a single FFM lease from the
 * {@code QueryMemoryManager}. Keeping that choice behind this interface allows callers to use the same lifecycle for
 * both implementations. Implementations must release any partially acquired resources when preparation fails.</p>
 */
@FunctionalInterface
public interface GroupByResourcesAllocator
{
  GroupByQueryResources prepareResource(
      QueryResourceId queryResourceId,
      GroupByQuery groupByQuery,
      boolean willMergeRunner,
      GroupByQueryConfig groupByQueryConfig
  );
}
