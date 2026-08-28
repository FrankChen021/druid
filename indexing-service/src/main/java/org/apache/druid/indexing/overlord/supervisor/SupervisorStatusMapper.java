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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;

/** Builds the common supervisor status representation used by the REST API and {@code sys.supervisors}. */
final class SupervisorStatusMapper
{
  private SupervisorStatusMapper()
  {
  }

  public static SupervisorStatus toStatus(
      final ObjectMapper objectMapper,
      final SupervisorManager manager,
      final String supervisorId,
      final boolean includeFull,
      final boolean includeSystem
  )
  {
    final SupervisorStatus.Builder builder = new SupervisorStatus.Builder().withId(supervisorId);
    final Optional<SupervisorStateManager.State> state = manager.getSupervisorState(supervisorId);
    if (state.isPresent()) {
      builder.withState(state.get().getBasicState().toString())
             .withDetailedState(state.get().toString())
             .withHealthy(state.get().isHealthy());
    }

    final Optional<SupervisorSpec> optionalSpec = manager.getSupervisorSpec(supervisorId);
    if (optionalSpec.isPresent()) {
      final SupervisorSpec spec = optionalSpec.get();
      builder.withDataSource(spec.getDataSources().stream().findFirst().orElse(null));
      if (includeFull) {
        builder.withSpec(spec);
      }
      if (includeSystem) {
        try {
          // Serialize the spec explicitly so consumers do not need bindings for every SupervisorSpec subtype.
          builder.withSpecString(objectMapper.writeValueAsString(spec));
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
        builder.withType(spec.getType())
               .withSource(spec.getSource())
               .withSuspended(spec.isSuspended());
      }
    }
    return builder.build();
  }
}
