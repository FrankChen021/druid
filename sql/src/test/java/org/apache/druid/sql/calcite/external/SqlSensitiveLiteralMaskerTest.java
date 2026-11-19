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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.remote.RemoteTlsContext;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SqlSensitiveLiteralMaskerTest
{
  private final SqlValidator validator = Mockito.mock(SqlValidator.class);
  private final SqlOperator remoteOperator = new RemoteOperatorConversion(
      new HttpInputSourceConfig(null, null),
      new RemoteTlsContext(),
      new DefaultObjectMapper()
  ).calciteOperator();

  @BeforeEach
  public void setUp()
  {
    Mockito.when(validator.getTypeFactory()).thenReturn(DruidTypeSystem.TYPE_FACTORY);
    Mockito.when(validator.getEmptyScope()).thenReturn(Mockito.mock(SqlValidatorScope.class));
  }

  @Test
  public void testMasksNamedAndPositionalLiteralsInOneQuery()
  {
    final String sql = """
        SELECT 'can''t-share' FROM TABLE(REMOTE(
            password => 'can''t-share',
            endpoint => 'https://source.example',
            dataSource => 'events',
            userName => 'reader',
            authType => 'basic'))
        UNION ALL
        SELECT a FROM TABLE(REMOTE('https://source.example', 'events', 'basic', 'reader', CAST('other' AS VARCHAR)))
        """.replace("\n", "\r\n");

    Assertions.assertEquals(
        sql.replace("password => 'can''t-share'", "password => '<masked>'")
           .replace("CAST('other' AS VARCHAR)", "CAST('<masked>' AS VARCHAR)"),
        mask(sql)
    );
  }

  @Test
  public void testMasksContinuedUnicodeLiteralIncludingEscapeClause()
  {
    final String password = "U&'first'\n 'secrem0074' /* custom escape follows */ UESCAPE 'm'";
    final String sql = "SELECT * FROM TABLE(REMOTE('https://source.example', 'events', 'basic', 'reader', "
                       + password + "))";

    final String maskedSql = mask(sql);
    Assertions.assertEquals(sql.replace(password, "'<masked>'"), maskedSql);
    DruidSqlParser.parse(maskedSql, false);
  }

  @Test
  public void testPreservesBoundPlaceholderInSqlText()
  {
    final String sql = "SELECT * FROM TABLE(REMOTE('https://source.example', 'events', 'basic', 'reader', ?))";

    Assertions.assertEquals(sql, mask(sql));
  }

  @Test
  public void testQueryWithoutSensitiveArguments()
  {
    final String sql = "SELECT 'password' FROM TABLE(REMOTE('https://source.example', 'events'))";

    Assertions.assertEquals(sql, mask(sql));
  }

  private String mask(final String sql)
  {
    // Model the resolved operator and early parameter binding without running the planner or fetching remote metadata.
    final SqlNode resolved = DruidSqlParser.parse(sql, false).getMainStatement().accept(new SqlShuttle()
    {
      @Override
      public SqlNode visit(final SqlCall call)
      {
        if (call instanceof SqlBasicCall basicCall && "REMOTE".equalsIgnoreCase(call.getOperator().getName())) {
          basicCall.setOperator(remoteOperator);
        }
        return super.visit(call);
      }

      @Override
      public SqlNode visit(final SqlDynamicParam param)
      {
        return SqlLiteral.createCharString("bound-password", param.getParserPosition());
      }
    });
    return SqlSensitiveLiteralMasker.mask(sql, resolved, validator);
  }
}
