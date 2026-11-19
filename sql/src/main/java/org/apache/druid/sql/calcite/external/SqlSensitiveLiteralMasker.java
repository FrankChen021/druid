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

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks sensitive table-function literals in a copy of the original SQL, preserving its formatting.
 */
public class SqlSensitiveLiteralMasker
{
  private static final Pattern LINE_ENDING = Pattern.compile("\\r\\n|[\\r\\n]");
  private static final String MASKED_LITERAL = "'<masked>'";

  private SqlSensitiveLiteralMasker()
  {
  }

  public static String mask(final String sql, final SqlNode validatedQuery, final SqlValidator validator)
  {
    final List<SqlParserPos> positions = new ArrayList<>();
    validatedQuery.accept(new SqlShuttle()
    {
      @Override
      public SqlNode visit(final SqlCall call)
      {
        if (!(call.getOperator() instanceof BaseUserDefinedTableMacro macro)) {
          return super.visit(call);
        }

        final List<FunctionParameter> parameters = macro.getParameters();
        List<SqlNode> operands = null;
        for (int i = 0; i < parameters.size(); i++) {
          if (!isSensitive(parameters.get(i))) {
            continue;
          }
          if (operands == null) {
            // Use Calcite's permutation so named and positional arguments bind exactly as they do during validation.
            operands = new SqlCallBinding(validator, validator.getEmptyScope(), call).operands();
          }
          if (i >= operands.size()) {
            break;
          }

          final SqlParserPos position = literalPosition(operands.get(i));
          if (position != null && position.getLineNum() > 0) {
            positions.add(position);
          }
        }
        return super.visit(call);
      }
    });

    if (positions.isEmpty()) {
      return sql;
    }

    // Parser coordinates are one-based and the ending column is inclusive.
    final List<Integer> lineStarts = new ArrayList<>();
    lineStarts.add(0);
    final Matcher lineEndings = LINE_ENDING.matcher(sql);
    while (lineEndings.find()) {
      lineStarts.add(lineEndings.end());
    }

    // Find out text ranges we need to mask
    final NavigableMap<Integer, Integer> ranges = new TreeMap<>();
    for (final SqlParserPos position : positions) {
      final int start = lineStarts.get(position.getLineNum() - 1) + position.getColumnNum() - 1;
      final int end = lineStarts.get(position.getEndLineNum() - 1) + position.getEndColumnNum();
      // Binding can turn a dynamic parameter into a literal with the original question mark's position.
      if (sql.charAt(start) != '?') {
        ranges.put(start, end);
      }
    }

    final StringBuilder masked = new StringBuilder(sql);
    for (final Map.Entry<Integer, Integer> range : ranges.descendingMap().entrySet()) {
      masked.replace(range.getKey(), range.getValue(), MASKED_LITERAL);
    }
    return masked.toString();
  }

  private static boolean isSensitive(final FunctionParameter parameter)
  {
    return parameter instanceof FunctionParameterImpl druidParameter && druidParameter.isSensitive();
  }

  @Nullable
  private static SqlParserPos literalPosition(@Nullable final SqlNode operand)
  {
    if (operand instanceof SqlCall call && call.getKind() == SqlKind.CAST) {
      // Descend through casts, such as those added by type coercion, to find the literal's span and preserve the
      // surrounding SQL.
      return literalPosition(call.operand(0));
    }

    if ((operand instanceof SqlLiteral literal && literal.getValue() != null)
        || (operand != null && operand.getKind() == SqlKind.LITERAL_CHAIN)) {
      return operand.getParserPosition();
    }
    return null;
  }
}
