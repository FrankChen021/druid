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
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Query;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.rpc.indexing.NativeSystemQueryResponse;
import org.apache.druid.server.security.AuthenticationResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/** Returns storage-prefiltered rows for validated native queries over component-owned system tables. */
@Path("/druid/v2/system")
public class NativeSystemQueryResource
{
  private final Map<String, NativeSystemTableDataSupplier> dataSuppliers;
  private final ServiceEmitter emitter;

  @Inject
  public NativeSystemQueryResource(
      final Map<String, NativeSystemTableDataSupplier> dataSuppliers,
      final ServiceEmitter emitter
  )
  {
    this.dataSuppliers = dataSuppliers;
    this.emitter = emitter;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public NativeSystemQueryResponse query(final Query<?> query, @Context final HttpServletRequest request)
  {
    if (!query.context().getBoolean(SystemTableDataSource.CTX_NATIVE_SYSTEM_QUERY_SCAN_ONLY, false)) {
      throw new WebApplicationException(
          Response.status(Response.Status.BAD_REQUEST)
                  .entity("Native system-table components only serve scan-only row requests")
                  .build()
      );
    }

    final NativeSystemTableDataSupplier dataSupplier = validateAndGetDataSupplier(query);
    final AuthenticationResult originalAuthenticationResult = originalAuthenticationResult(query);
    final Iterable<Object[]> rows = dataSupplier.getRows(
        NativeSystemTableFilterExtractor.extract(query, dataSupplier.getFilterRules()),
        request,
        originalAuthenticationResult
    );
    final List<Object[]> rowList = Sequences.simple(rows).toList();
    final NativeSystemQueryResponse response = new NativeSystemQueryResponse(dataSupplier.getRowSignature(), rowList);

    final String metricPrefix = metricPrefix(((SystemTableDataSource) query.getDataSource()).getTable());
    emitter.emit(ServiceMetricEvent.builder().setMetric("query/" + metricPrefix + "/rowsRead", rowList.size()));
    emitter.emit(
        ServiceMetricEvent.builder().setMetric("query/" + metricPrefix + "/rowsReturned", response.getRows().size())
    );
    return response;
  }

  private NativeSystemTableDataSupplier validateAndGetDataSupplier(final Query<?> query)
  {
    if (!(query instanceof GroupByQuery || query instanceof ScanQuery || query instanceof WindowOperatorQuery)
        || !(query.getDataSource() instanceof SystemTableDataSource)) {
      throw unsupportedQuery();
    }

    final String table = ((SystemTableDataSource) query.getDataSource()).getTable();
    final NativeSystemTableDataSupplier dataSupplier = dataSuppliers.get(table);
    if (dataSupplier == null) {
      throw new WebApplicationException(
          Response.status(501)
                  .entity("Native system table is not served by this component")
                  .build()
      );
    }
    return dataSupplier;
  }

  @SuppressWarnings("unchecked")
  private static AuthenticationResult originalAuthenticationResult(final Query<?> query)
  {
    return new AuthenticationResult(
        query.context().getString(SystemTableDataSource.CTX_AUTHENTICATION_IDENTITY),
        query.context().getString(SystemTableDataSource.CTX_AUTHENTICATION_AUTHORIZER),
        query.context().getString(SystemTableDataSource.CTX_AUTHENTICATED_BY),
        (Map<String, Object>) query.context().get(SystemTableDataSource.CTX_AUTHENTICATION_CONTEXT)
    );
  }

  private static WebApplicationException unsupportedQuery()
  {
    return new WebApplicationException(
        Response.status(Response.Status.BAD_REQUEST).entity("Unsupported native system query").build()
    );
  }

  private static String metricPrefix(final String table)
  {
    final StringBuilder metricPrefix = new StringBuilder("system");
    boolean capitalize = true;
    for (int i = 0; i < table.length(); i++) {
      final char character = table.charAt(i);
      if (character == '_') {
        capitalize = true;
      } else {
        metricPrefix.append(capitalize ? Character.toUpperCase(character) : character);
        capitalize = false;
      }
    }
    return metricPrefix.toString();
  }
}
