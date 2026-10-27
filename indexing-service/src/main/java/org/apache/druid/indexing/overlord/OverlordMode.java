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

package org.apache.druid.indexing.overlord;

/**
 * How the Overlord is deployed. Bound by {@code CliOverlord}, which knows whether it
 * was started as a standalone service or as part of a Coordinator, so that this does
 * not depend on {@code druid.coordinator.asOverlord.enabled}, a Coordinator-side
 * config that a standalone Overlord may also see in a shared runtime.properties.
 */
public enum OverlordMode
{
  /**
   * The Overlord runs as a separate service.
   */
  STANDALONE,

  /**
   * The Overlord runs inside a Coordinator, i.e. {@code druid.coordinator.asOverlord.enabled=true}
   * on the Coordinator. Services shared with the Coordinator, such as the segment metadata cache,
   * are started and stopped by the Coordinator.
   */
  EMBEDDED_IN_COORDINATOR
}
