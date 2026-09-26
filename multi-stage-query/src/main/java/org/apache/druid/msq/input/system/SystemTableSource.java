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

package org.apache.druid.msq.input.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;

import java.util.Objects;
import java.util.Set;

/** A physical node and its roles selected as a source for one system-table input. */
public class SystemTableSource
{
  private final DruidNode node;
  private final Set<NodeRole> nodeRoles;

  @JsonCreator
  public SystemTableSource(
      @JsonProperty("node") final DruidNode node,
      @JsonProperty("nodeRoles") final Set<NodeRole> nodeRoles
  )
  {
    this.node = Preconditions.checkNotNull(node, "node");
    this.nodeRoles = Set.copyOf(Preconditions.checkNotNull(nodeRoles, "nodeRoles"));
  }

  @JsonProperty
  public DruidNode getNode()
  {
    return node;
  }

  @JsonProperty
  public Set<NodeRole> getNodeRoles()
  {
    return nodeRoles;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SystemTableSource that = (SystemTableSource) o;
    return Objects.equals(node, that.node) && Objects.equals(nodeRoles, that.nodeRoles);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(node, nodeRoles);
  }

  @Override
  public String toString()
  {
    return "SystemTableSource{" +
           "node=" + node +
           ", nodeRoles=" + nodeRoles +
           '}';
  }
}
