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

import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.RemoteDruidAuthentication;
import org.apache.druid.data.input.impl.RemoteDruidConnection;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.DruidSqlValidator;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerToolbox;
import org.apache.druid.sql.calcite.table.ExternalTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class DruidOperatorConversionTest
{
  private final DruidTableMacro macro = (DruidTableMacro) ((BaseUserDefinedTableMacro) new DruidOperatorConversion(
      new HttpInputSourceConfig(null, null), new DefaultObjectMapper()
  ).calciteOperator()).macro;

  private TranslatableTable apply(final Map<String, Object> options, final SqlNodeList schema)
  {
    final Map<String, Object> args = new HashMap<>(options);
    args.put("endpoint", "https://source.example");
    args.put("dataSource", "events");
    return macro.apply(macro.getParameters().stream().map(p -> args.get(p.getName())).toList(), schema);
  }

  private static SqlNodeList schema(final SqlTypeName type)
  {
    return new SqlNodeList(List.of(
        new SqlIdentifier("__time", SqlParserPos.ZERO),
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type, SqlParserPos.ZERO), SqlParserPos.ZERO)
    ), SqlParserPos.ZERO);
  }

  private static RemoteDruidConnection connection(final TranslatableTable table)
  {
    return ((RemoteDruidInputSource) ((ExternalDataSource) ((ExternalTable) table).getDataSource()).getInputSource()).getConnection();
  }

  @Test
  public void testExplicitSchemaAndAuthenticationSettings()
  {
    final RemoteDruidConnection anonymous = connection(apply(Map.of(), schema(SqlTypeName.BIGINT)));
    Assertions.assertInstanceOf(RemoteDruidAuthentication.None.class, anonymous.getAuthentication());
    Assertions.assertEquals(URI.create("https://source.example/druid/v2"), anonymous.getEndpoint());
    final RemoteDruidConnection basic = connection(apply(Map.of(
        "authType", "basic", "username", "reader", "password", "secret",
        "connectTimeoutMillis", 123L, "readTimeoutMillis", 456L, "maxResponseBytes", 789L, "maxRetries", 0L
    ), schema(SqlTypeName.BIGINT)));
    final RemoteDruidAuthentication.Basic authentication = Assertions.assertInstanceOf(
        RemoteDruidAuthentication.Basic.class,
        basic.getAuthentication()
    );
    Assertions.assertEquals("reader", authentication.getUsername());
    Assertions.assertEquals("secret", authentication.getPassword().getPassword());
    Assertions.assertEquals(123, basic.getConnectTimeout());
    Assertions.assertEquals(456, basic.getReadTimeout());
    Assertions.assertEquals(789, basic.getMaxResponseBytes());
    Assertions.assertEquals(0, basic.getMaxRetries());
  }

  @Test
  public void testInvalidAuthenticationArguments()
  {
    for (final Map<String, Object> options : List.<Map<String, Object>>of(
        Map.of("username", "reader", "password", "secret"),
        Map.of("authType", "basic", "username", "reader"),
        Map.of("authType", "basic", "password", "secret"),
        Map.of("authType", "unsupported", "password", "secret")
    )) {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> apply(options, schema(SqlTypeName.BIGINT))
      );
      Assertions.assertFalse(exception.getMessage().contains("secret"));
    }
  }

  @Test
  public void testInvalidReadArgumentsAndTimeSchema()
  {
    for (final Map<String, Object> options : List.<Map<String, Object>>of(
        Map.of("connectTimeoutMillis", 0L),
        Map.of("readTimeoutMillis", Long.MAX_VALUE),
        Map.of("maxResponseBytes", 0L),
        Map.of("maxRetries", -1L),
        Map.of("splitDurationMillis", 0L),
        Map.of("intervals", List.of("invalid"))
    )) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> apply(options, schema(SqlTypeName.BIGINT)));
    }
    Assertions.assertThrows(IllegalArgumentException.class, () -> apply(Map.of(), schema(SqlTypeName.VARCHAR)));
    Assertions.assertThrows(IllegalArgumentException.class, () -> apply(Map.of(), SqlNodeList.EMPTY));
  }

  @Test
  public void testDeniedExternalPermissionPreventsDiscovery()
  {
    for (final boolean typeSecurity : List.of(false, true)) {
      final AtomicReference<Resource> checkedResource = new AtomicReference<>();
      final AuthorizerMapper authorizers = new AuthorizerMapper(Map.of("test", (authentication, resource, action) -> {
        checkedResource.set(resource);
        return Access.DENIED;
      }));
      final PlannerToolbox toolbox = Mockito.mock(PlannerToolbox.class);
      Mockito.when(toolbox.getAuthConfig()).thenReturn(AuthConfig.newBuilder().setEnableInputSourceSecurity(typeSecurity).build());
      Mockito.when(toolbox.getAuthorizerMapper()).thenReturn(authorizers);
      final PlannerContext context = Mockito.mock(PlannerContext.class);
      Mockito.when(context.getPlannerToolbox()).thenReturn(toolbox);
      Mockito.when(context.getAuthenticationResult()).thenReturn(new AuthenticationResult("reader", "test", "test", null));
      final DruidSqlValidator validator = Mockito.mock(DruidSqlValidator.class);
      Mockito.when(validator.getPlannerContext()).thenReturn(context);
      final SqlCallBinding binding = Mockito.mock(SqlCallBinding.class);
      Mockito.when(binding.getValidator()).thenReturn(validator);
      final BaseUserDefinedTableMacro operator = (BaseUserDefinedTableMacro) new DruidOperatorConversion(
          new HttpInputSourceConfig(null, null), new DefaultObjectMapper()
      ).calciteOperator();
      final DruidUserDefinedTableMacro remote = (DruidUserDefinedTableMacro) operator;
      Assertions.assertEquals(
          Set.of(typeSecurity ? AuthorizationUtils.createExternalResourceReadAction("remoteDruid")
                              : Externals.EXTERNAL_RESOURCE_ACTION),
          remote.computeResources(null, typeSecurity)
      );
      Assertions.assertThrows(ForbiddenException.class, () -> operator.getTable(binding));
      Assertions.assertEquals(new Resource(typeSecurity ? "remoteDruid" : "EXTERNAL", ResourceType.EXTERNAL), checkedResource.get());
      // No argument conversion or network discovery is reached for a denied caller.
      Mockito.verify(binding, Mockito.never()).getOperandCount();
    }
  }
}
