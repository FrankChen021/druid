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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.RemoteDruidAuthentication;
import org.apache.druid.data.input.impl.RemoteDruidConnection;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.DruidSqlValidator;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// The `DRUID` table function for remote Druid input, with an optional explicit `EXTEND` schema.
public class DruidOperatorConversion extends DruidUserDefinedTableMacroConversion
{
  public static final String FUNCTION_NAME = "druid";

  @Inject
  public DruidOperatorConversion(final HttpInputSourceConfig config, @Json final ObjectMapper mapper)
  {
    super(new RemoteTableMacro(new DruidTableMacro(FUNCTION_NAME, new RemoteTableFunction(config), mapper)));
  }

  private static class RemoteTableMacro extends DruidUserDefinedTableMacro
  {
    RemoteTableMacro(final DruidTableMacro macro)
    {
      super(macro);
    }

    @Override
    public TranslatableTable getTable(final SqlOperatorBinding binding)
    {
      // Schema discovery performs HTTP during validation, before the normal statement authorization.
      if (!(binding instanceof SqlCallBinding call) || !(call.getValidator() instanceof DruidSqlValidator validator)) {
        throw new ForbiddenException();
      }
      final PlannerContext context = validator.getPlannerContext();
      final boolean typeSecurity = context.getPlannerToolbox().getAuthConfig().isEnableInputSourceSecurity();
      final ResourceAction resource = externalResource(typeSecurity);
      if (!AuthorizationUtils.authorizeResourceAction(
          context.getAuthenticationResult(), resource, context.getPlannerToolbox().getAuthorizerMapper()
      ).allowAccessWithNoRestriction()) {
        throw new ForbiddenException();
      }
      return super.getTable(binding);
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call, final boolean inputSourceTypeSecurityEnabled)
    {
      return Set.of(externalResource(inputSourceTypeSecurityEnabled));
    }

    private static ResourceAction externalResource(final boolean typeSecurity)
    {
      return typeSecurity ? AuthorizationUtils.createExternalResourceReadAction(RemoteDruidInputSource.TYPE_KEY)
                          : Externals.EXTERNAL_RESOURCE_ACTION;
    }
  }

  private static class RemoteTableFunction extends BaseTableFunction
  {
    private final HttpInputSourceConfig config;

    RemoteTableFunction(final HttpInputSourceConfig config)
    {
      super(List.of(
          new Parameter("endpoint", ParameterType.VARCHAR, false),
          new Parameter("dataSource", ParameterType.VARCHAR, false),
          new Parameter("authType", ParameterType.VARCHAR, true),
          new Parameter("username", ParameterType.VARCHAR, true),
          new Parameter("password", ParameterType.VARCHAR, true),
          new Parameter("intervals", ParameterType.VARCHAR_ARRAY, true),
          new Parameter("connectTimeoutMillis", ParameterType.BIGINT, true),
          new Parameter("readTimeoutMillis", ParameterType.BIGINT, true),
          new Parameter("maxRetries", ParameterType.BIGINT, true),
          new Parameter("maxResponseBytes", ParameterType.BIGINT, true)
      ));
      this.config = config;
    }

    @Override
    public ExternalTableSpec apply(
        final String fnName,
        final Map<String, Object> args,
        @Nullable final List<ColumnSpec> columns,
        final ObjectMapper jsonMapper
    )
    {
      final String username = CatalogUtils.getString(args, "username");
      final String password = CatalogUtils.getString(args, "password");
      final String requestedAuthType = CatalogUtils.getString(args, "authType");
      final String authType = requestedAuthType == null ? "none" : requestedAuthType;
      final RemoteDruidAuthentication authentication;
      if ("none".equals(authType)) {
        if (username != null || password != null) {
          throw new IAE("Authentication type [none] does not accept credentials");
        }
        authentication = new RemoteDruidAuthentication.None();
      } else if ("basic".equals(authType)) {
        if (username == null || password == null) {
          throw new IAE("Authentication type [basic] requires username and password");
        }
        authentication = new RemoteDruidAuthentication.Basic(username, new DefaultPasswordProvider(password));
      } else {
        throw new IAE("Unsupported remote authentication type");
      }
      final URI endpoint;
      try {
        endpoint = URI.create(CatalogUtils.getString(args, "endpoint"));
      }
      catch (RuntimeException e) {
        throw new IAE("Invalid remote Druid endpoint");
      }
      final RemoteDruidConnection connection = new RemoteDruidConnection(
          endpoint,
          authentication,
          integerArg(args, "connectTimeoutMillis"),
          integerArg(args, "readTimeoutMillis"),
          longArg(args, "maxResponseBytes"),
          integerArg(args, "maxRetries")
      );
      final List<String> intervalStrings = CatalogUtils.getStringArray(args, "intervals");
      final List<Interval> intervals;
      try {
        intervals = intervalStrings == null ? null : intervalStrings.stream().map(Intervals::of).toList();
      }
      catch (RuntimeException e) {
        throw new IAE("Invalid remote Druid intervals");
      }
      final RemoteDruidInputSource source = new RemoteDruidInputSource(
          connection,
          CatalogUtils.getString(args, "dataSource"),
          intervals,
          config,
          jsonMapper
      );
      final RowSignature signature;
      if (columns != null) {
        final RowSignature.Builder builder = RowSignature.builder();
        for (final ColumnSpec column : columns) {
          builder.add(column.name(), Columns.druidTypeFromString(column.dataType()));
        }
        signature = builder.build();
      } else {
        try {
          signature = source.discoverSchema();
        }
        catch (IOException e) {
          throw new IAE("Remote Druid schema discovery failed: %s", e.getMessage());
        }
      }
      if (!ColumnType.LONG.equals(signature.getColumnType("__time").orElse(null))) {
        throw new IAE("DRUID requires an __time column of type BIGINT");
      }
      for (final String name : signature.getColumnNames()) {
        final ColumnType type = signature.getColumnType(name).orElse(null);
        if (type == null || !(type.isPrimitive() || type.isPrimitiveArray())) {
          throw new IAE("DRUID supports only primitive and primitive-array columns");
        }
      }
      return new ExternalTableSpec(source, null, signature, source::getTypes);
    }

    @Nullable
    private static Long longArg(final Map<String, Object> args, final String name)
    {
      return args.containsKey(name) ? CatalogUtils.getLong(args, name) : null;
    }

    @Nullable
    private static Integer integerArg(final Map<String, Object> args, final String name)
    {
      final Long value = longArg(args, name);
      if (value == null) {
        return null;
      }
      if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
        throw new IAE("Remote Druid argument [%s] exceeds the supported integer range", name);
      }
      return value.intValue();
    }
  }
}
