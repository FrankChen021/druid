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

package org.apache.druid.indexing.overlord.http;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;

/**
 * The query stack used by components that expose system tables only needs the scan tool chest. In particular, it must
 * not construct the normal query factory map, which eagerly provisions the groupBy processing buffers.
 */
public class NativeSystemQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
{
  private final ScanQueryQueryToolChest scanQueryToolChest;

  @Inject
  public NativeSystemQueryRunnerFactoryConglomerate(final ScanQueryQueryToolChest scanQueryToolChest)
  {
    this.scanQueryToolChest = scanQueryToolChest;
  }

  @Override
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(final QueryType query)
  {
    throw unsupportedQuery(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
  {
    if (query instanceof ScanQuery) {
      return (QueryToolChest<T, QueryType>) scanQueryToolChest;
    }
    throw unsupportedQuery(query);
  }

  @Override
  public <T, QueryType extends Query<T>> QueryLogic getQueryLogic(final QueryType query)
  {
    throw unsupportedQuery(query);
  }

  private static ISE unsupportedQuery(final Query<?> query)
  {
    return new ISE(
        "Only Scan queries are supported by the component-local native system query endpoint, got[%s]",
        query.getType()
    );
  }
}
