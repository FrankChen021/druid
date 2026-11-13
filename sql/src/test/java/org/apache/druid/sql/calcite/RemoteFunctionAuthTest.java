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

package org.apache.druid.sql.calcite;

import com.sun.net.httpserver.HttpServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Verifies who may use a remote Druid table, through {@code REMOTE} or through {@code EXTERN} with a remote input
 * source, and how the source rejecting the configured credentials is reported.
 * <p>
 * Without input source security, a remote table needs {@code EXTERNAL} read access. With it, the remote table needs
 * read access to the {@code remote} input source type specifically, so a grant on another type, such as {@code http},
 * is not enough; the tests pin down the exact resources that are checked. In both modes the target datasource needs
 * write access as usual.
 */
public class RemoteFunctionAuthTest extends CalciteIngestionDmlTest
{
  private static final String REMOTE_TABLE = """
      TABLE(REMOTE(endpoint => 'https://source.example', dataSource => 'events'))
        EXTEND (__time BIGINT, a VARCHAR, m BIGINT)
      """;

  private static final String EXTERN_REMOTE_TABLE = """
      TABLE(EXTERN(
        '{"type":"remote","connection":{"endpoint":"https://source.example"},"dataSource":"events"}',
        'null',
        '[{"name":"__time","type":"long"},{"name":"a","type":"string"},{"name":"m","type":"long"}]'
      ))
      """;

  private static final RowSignature TARGET_SIGNATURE = RowSignature.builder()
                                                                   .add("__time", ColumnType.LONG)
                                                                   .add("a", ColumnType.STRING)
                                                                   .add("m", ColumnType.LONG)
                                                                   .build();

  private static final AuthConfig INPUT_SOURCE_SECURITY = AuthConfig.newBuilder()
                                                                    .setEnableInputSourceSecurity(true)
                                                                    .build();

  private static String insertFrom(final String table)
  {
    return StringUtils.format("INSERT INTO dst SELECT * FROM %s PARTITIONED BY DAY", table);
  }

  @Test
  public void testRemoteDiscoveryRequiresExternalPermission()
  {
    // Without EXTEND the schema is discovered from the source while the statement is validated. The caller is
    // checked first, so the unreachable source is never contacted and the failure is a permission error.
    testIngestionQuery()
        .sql(insertFrom("TABLE(REMOTE(endpoint => 'https://source.example', dataSource => 'events'))"))
        .authentication(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectValidationError(
            new DruidExceptionMatcher(DruidException.Persona.USER, DruidException.Category.FORBIDDEN, "forbidden")
        )
        .verify();
  }

  @Test
  public void testRemoteWithExternalPermission()
  {
    testIngestionQuery()
        .sql(insertFrom(REMOTE_TABLE))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", TARGET_SIGNATURE)
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .verify();
  }

  @Test
  public void testRemoteWithInputSourceSecurityAndRemotePermission()
  {
    // The only input source permission requested is the remote type, so a grant on another type does not apply.
    testIngestionQuery()
        .sql(insertFrom(REMOTE_TABLE))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(INPUT_SOURCE_SECURITY)
        .expectTarget("dst", TARGET_SIGNATURE)
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), externalRead("remote"))
        .verify();
  }

  @Test
  public void testExternRemoteWithInputSourceSecurityAndRemotePermission()
  {
    testIngestionQuery()
        .sql(insertFrom(EXTERN_REMOTE_TABLE))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authConfig(INPUT_SOURCE_SECURITY)
        .expectTarget("dst", TARGET_SIGNATURE)
        .expectResources(dataSourceRead("dst"), dataSourceWrite("dst"), externalRead("remote"))
        .verify();
  }

  @Test
  public void testRejectedCredentialsAreInvalidInput() throws IOException
  {
    final HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/druid/v2", exchange -> {
      try (exchange) {
        final byte[] body = StringUtils.toUtf8("{\"error\":\"Unauthorized\"}");
        exchange.sendResponseHeaders(401, body.length);
        exchange.getResponseBody().write(body);
      }
    });
    server.start();
    try {
      testIngestionQuery()
          .sql(insertFrom(StringUtils.format(
              "TABLE(REMOTE(endpoint => 'http://localhost:%d', dataSource => 'events'))",
              server.getAddress().getPort()
          )))
          .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
          .expectValidationError(
              DruidExceptionMatcher.invalidInput()
                                   .expectMessageContains("rejected the credentials")
                                   .expectMessageContains("Unauthorized")
          )
          .verify();
    }
    finally {
      server.stop(0);
    }
  }
}
