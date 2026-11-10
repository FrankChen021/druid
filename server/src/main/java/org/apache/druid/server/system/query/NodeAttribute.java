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

package org.apache.druid.server.system.query;

import org.apache.druid.server.system.table.SystemTableDescriptor;

/**
 * An attribute of a Druid service process that a column of a system table can hold, so the Broker can choose which
 * services to query from a filter on that column. See {@link SystemTableDescriptor#getNodeColumns()}.
 *
 * <p>{@link #getValue} assumes that a process has one value of the attribute. An attribute with several values per
 * process, such as its node roles (a Coordinator running as an Overlord has two), would need the method widened to
 * return a set.</p>
 */
public enum NodeAttribute
{
  /**
   * The {@code host:port} that peers use to reach the process, as in {@link
   * org.apache.druid.server.DruidNode#getHostAndPortToUse()}: the TLS port if it is enabled, else the plaintext port.
   * Providers report their own {@code @Self} node the same way, and services announce that node in discovery, so the
   * values agree.
   */
  HOST_AND_PORT {
    @Override
    String getValue(final SystemTableNode node)
    {
      return node.getDiscoveryNode().getDruidNode().getHostAndPortToUse();
    }
  },

  /**
   * The service name of the process, as defined by {@code druid.service}.
   */
  SERVICE_NAME {
    @Override
    String getValue(final SystemTableNode node)
    {
      return node.getDiscoveryNode().getDruidNode().getServiceName();
    }
  };

  /**
   * Returns the value of the attribute for the process, which has one value per process.
   */
  abstract String getValue(SystemTableNode node);
}
