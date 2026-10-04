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

package org.apache.druid.msq.sql;

import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.run.QueryMaker;

/// Runs the MSQ target task using the preplanned scan of a remote source result.
public final class RemoteDruidQueryMaker implements QueryMaker, QueryMaker.FromDruidLogical
{
  private final QueryMaker delegate;
  private final DruidQuery remoteFramesQuery;

  public RemoteDruidQueryMaker(final QueryMaker delegate, final DruidQuery remoteFramesQuery)
  {
    this.delegate = delegate;
    this.remoteFramesQuery = remoteFramesQuery;
  }

  @Override
  public QueryResponse<Object[]> runQuery(final DruidQuery druidQuery)
  {
    return delegate.runQuery(remoteFramesQuery);
  }

  @Override
  public QueryResponse<Object[]> runQuery(final DruidLogicalNode newRoot)
  {
    return delegate.runQuery(remoteFramesQuery);
  }
}
