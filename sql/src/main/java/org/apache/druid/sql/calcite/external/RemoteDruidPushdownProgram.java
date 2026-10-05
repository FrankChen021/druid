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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.Program;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.data.input.impl.RemoteDruidSqlInputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.builtin.MillisToTimestampOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.ExternalTable;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// Pushes the relational operators directly above each `DRUID(...)` table scan to the source cluster.
///
/// The longest chain of [Project], [Filter], and [Aggregate] operators above a remote scan is converted to one source
/// SELECT, and the chain is replaced by an ordinary [ExternalTableScan] of a [RemoteDruidSqlInputSource] that streams
/// that SELECT's result. Operators that the chain cannot absorb, such as joins, sorts, and window functions, stay on
/// the target. Because the rewrite happens before native query generation, EXPLAIN shows the plan that runs.
public class RemoteDruidPushdownProgram implements Program
{
  /// Query context keys forwarded to the source so that it evaluates SQL with the same semantics.
  private static final Set<String> FORWARDED_CONTEXT_KEYS = Set.of(
      "sqlUseApproximateCountDistinct",
      "sqlUseApproximateTopN",
      "useApproximateCountDistinct",
      "useApproximateTopN",
      "timeout",
      "priority"
  );

  /// Druid SQL accepts Calcite's default syntax with double-quoted, case-sensitive identifiers and no charsets.
  static final SqlDialect DIALECT = new SqlDialect(
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE)
          .withIdentifierQuoteString("\"")
          .withLiteralQuoteString("'")
          .withLiteralEscapedQuoteString("''")
          .withUnquotedCasing(Casing.UNCHANGED)
          .withQuotedCasing(Casing.UNCHANGED)
          .withCaseSensitive(true)
          .withNullCollation(NullCollation.HIGH)
  )
  {
    @Override
    public boolean supportsCharSet()
    {
      return false;
    }
  };

  private final PlannerContext plannerContext;

  public RemoteDruidPushdownProgram(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public RelNode run(
      final RelOptPlanner planner,
      final RelNode rel,
      final RelTraitSet requiredOutputTraits,
      final List<RelOptMaterialization> materializations,
      final List<RelOptLattice> lattices
  )
  {
    if (!containsRemoteScan(rel)) {
      return rel;
    }
    // Move filters above joins into the join inputs first, so that remote inputs can absorb them.
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addMatchLimit(CalciteRulesManager.HEP_DEFAULT_MATCH_LIMIT);
    builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
    builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
    final HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.setRoot(rel);
    return rewrite(hepPlanner.findBestExp());
  }

  private static boolean containsRemoteScan(final RelNode node)
  {
    if (isRemoteScan(node)) {
      return true;
    }
    for (final RelNode input : node.getInputs()) {
      if (containsRemoteScan(input)) {
        return true;
      }
    }
    return false;
  }

  private RelNode rewrite(final RelNode node)
  {
    final List<RelNode> chain = pushableChain(node);
    if (chain != null) {
      return pushDown(chain);
    }

    boolean changed = false;
    final List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
    for (final RelNode input : node.getInputs()) {
      final RelNode newInput = rewrite(input);
      changed |= newInput != input;
      newInputs.add(newInput);
    }
    return changed ? node.copy(node.getTraitSet(), newInputs) : node;
  }

  /// Returns the operators from `node` down to a remote scan, top first, if all of them can run on the source.
  @Nullable
  private static List<RelNode> pushableChain(final RelNode node)
  {
    final List<RelNode> chain = new ArrayList<>();
    RelNode current = node;
    while (!isRemoteScan(current)) {
      if (!isPushable(current)) {
        return null;
      }
      chain.add(current);
      current = current.getInput(0);
    }
    chain.add(current);
    return chain;
  }

  private static boolean isRemoteScan(final RelNode node)
  {
    return node instanceof ExternalTableScan scan
           && scan.getDruidTable().getDataSource() instanceof ExternalDataSource dataSource
           && dataSource.getInputSource() instanceof RemoteDruidInputSource;
  }

  private static boolean isPushable(final RelNode node)
  {
    final List<RexNode> expressions;
    if (node instanceof Project project) {
      expressions = project.getProjects();
    } else if (node instanceof Filter filter) {
      expressions = List.of(filter.getCondition());
    } else if (node instanceof Aggregate aggregate) {
      if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
        return false;
      }
      for (final AggregateCall call : aggregate.getAggCallList()) {
        if (!call.getCollation().getFieldCollations().isEmpty() || call.rexList != null && !call.rexList.isEmpty()) {
          return false;
        }
      }
      expressions = List.of();
    } else {
      return false;
    }

    for (final RexNode expression : expressions) {
      if (!isPushable(expression)) {
        return false;
      }
    }
    return hasSupportedTypes(node.getRowType());
  }

  private static boolean isPushable(final RexNode expression)
  {
    final boolean[] pushable = {true};
    expression.accept(new RexVisitorImpl<Void>(true)
    {
      @Override
      public Void visitCall(final RexCall call)
      {
        // Lookups are configured per cluster, so the source might resolve them differently.
        if ("LOOKUP".equalsIgnoreCase(call.getOperator().getName())) {
          pushable[0] = false;
        }
        return super.visitCall(call);
      }

      @Override
      public Void visitOver(final RexOver over)
      {
        pushable[0] = false;
        return null;
      }

      @Override
      public Void visitSubQuery(final RexSubQuery subQuery)
      {
        pushable[0] = false;
        return null;
      }

      @Override
      public Void visitCorrelVariable(final RexCorrelVariable correlVariable)
      {
        pushable[0] = false;
        return null;
      }

      @Override
      public Void visitDynamicParam(final RexDynamicParam dynamicParam)
      {
        pushable[0] = false;
        return null;
      }
    });
    return pushable[0];
  }

  private static boolean hasSupportedTypes(final RelDataType rowType)
  {
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    for (final String column : signature.getColumnNames()) {
      final ColumnType type = signature.getColumnType(column).orElse(null);
      if (type == null || !(type.isPrimitive() || type.isPrimitiveArray())) {
        return false;
      }
    }
    return true;
  }

  private RelNode pushDown(final List<RelNode> chain)
  {
    final ExternalTableScan remoteScan = (ExternalTableScan) chain.get(chain.size() - 1);
    final RemoteDruidInputSource remote =
        (RemoteDruidInputSource) ((ExternalDataSource) remoteScan.getDruidTable().getDataSource()).getInputSource();
    final RelNode top = chain.get(0);
    // Without aggregation, results for disjoint time ranges can be read independently and concatenated.
    final boolean timeRangeParameters = chain.stream().noneMatch(node -> node instanceof Aggregate);

    RelNode source = sourceLeaf(remoteScan, remote, timeRangeParameters);
    for (int i = chain.size() - 2; i >= 0; i--) {
      source = chain.get(i).copy(chain.get(i).getTraitSet(), List.of(source));
    }
    final String sql = toSql(source, remoteScan.getRowType());

    final RelDataType rowType = top.getRowType();
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    final List<String> timestampColumns = new ArrayList<>();
    for (final RelDataTypeField field : rowType.getFieldList()) {
      final SqlTypeName typeName = field.getType().getSqlTypeName();
      if (typeName == SqlTypeName.TIMESTAMP
          || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          || typeName == SqlTypeName.DATE) {
        timestampColumns.add(field.getName());
      }
    }

    final RemoteDruidSqlInputSource inputSource = remote.toSqlInputSource(
        sql,
        timeRangeParameters,
        signature,
        timestampColumns,
        sourceContext()
    );
    final ExternalTable table = new RemoteDruidSqlTable(
        new ExternalDataSource(inputSource, null, signature),
        signature,
        plannerContext,
        rowType
    );
    return new ExternalTableScan(remoteScan.getCluster(), plannerContext.getJsonMapper(), table);
  }

  /// Creates the source table scan, filtered to the requested time range.
  private RelNode sourceLeaf(
      final ExternalTableScan remoteScan,
      final RemoteDruidInputSource remote,
      final boolean timeRangeParameters
  )
  {
    final RelDataType rowType = remoteScan.getRowType();
    final RelOptTableImpl table = RelOptTableImpl.create(
        null,
        rowType,
        ImmutableList.of(remote.getDataSource()),
        Expressions.constant(new AbstractTable()
        {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory typeFactory)
          {
            return rowType;
          }
        })
    );
    final RelNode scan = LogicalTableScan.create(remoteScan.getCluster(), table, ImmutableList.of());

    final int timeColumn = rowType.getFieldNames().indexOf(ColumnHolder.TIME_COLUMN_NAME);
    if (!timeRangeParameters && remote.getIntervals() == null) {
      return scan;
    }
    if (timeColumn < 0) {
      throw InvalidInput.exception("DRUID tables must include the [%s] column", ColumnHolder.TIME_COLUMN_NAME);
    }

    final RexBuilder rexBuilder = remoteScan.getCluster().getRexBuilder();
    final RelDataType bigint = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    final RexNode condition;
    if (timeRangeParameters) {
      condition = timeRange(
          rexBuilder,
          rexBuilder.makeInputRef(scan, timeColumn),
          rexBuilder.makeDynamicParam(bigint, 0),
          rexBuilder.makeDynamicParam(bigint, 1)
      );
    } else {
      final List<RexNode> ranges = new ArrayList<>();
      for (final Interval interval : remote.getIntervals()) {
        if (Intervals.isEternity(interval)) {
          return scan;
        }
        ranges.add(timeRange(
            rexBuilder,
            rexBuilder.makeInputRef(scan, timeColumn),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(interval.getStartMillis()), bigint),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(interval.getEndMillis()), bigint)
        ));
      }
      condition = RexUtil.composeDisjunction(rexBuilder, ranges);
    }
    return LogicalFilter.create(scan, condition);
  }

  private static RexNode timeRange(
      final RexBuilder rexBuilder,
      final RexNode time,
      final RexNode startMillis,
      final RexNode endMillis
  )
  {
    final SqlOperator millisToTimestamp = new MillisToTimestampOperatorConversion().calciteOperator();
    return rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            time,
            rexBuilder.makeCall(millisToTimestamp, startMillis)
        ),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, time, rexBuilder.makeCall(millisToTimestamp, endMillis))
    );
  }

  /// Converts the source fragment to Druid SQL, listing source columns explicitly, since `SELECT *` on the source
  /// table would return every remote column rather than the declared ones.
  static String toSql(final RelNode source, final RelDataType tableRowType)
  {
    final SqlNode sqlNode = new RelToSqlConverter(DIALECT).visitRoot(source).asStatement();
    final SqlNode expanded = sqlNode.accept(new SqlShuttle()
    {
      @Override
      public SqlNode visit(final SqlCall call)
      {
        if (call instanceof SqlSelect select && isTableReference(select.getFrom())) {
          final SqlNodeList selectList = select.getSelectList();
          if (selectList == null || (selectList.size() == 1 && isStar(selectList.get(0)))) {
            final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
            for (final String column : tableRowType.getFieldNames()) {
              columns.add(new SqlIdentifier(column, SqlParserPos.QUOTED_ZERO));
            }
            select.setSelectList(columns);
          }
        }
        return super.visit(call);
      }

      @Override
      public SqlNode visit(final SqlIdentifier identifier)
      {
        // Calcite leaves "safe" identifiers unquoted, but an unquoted column such as `user` would be parsed by the
        // source as the USER function. Quote every name so the source reads exactly the columns planned here.
        if (identifier.isStar()) {
          return identifier;
        }
        return new SqlIdentifier(
            identifier.names,
            null,
            SqlParserPos.QUOTED_ZERO,
            Collections.nCopies(identifier.names.size(), SqlParserPos.QUOTED_ZERO)
        );
      }
    });
    return expanded.toSqlString(DIALECT).getSql();
  }

  private static boolean isTableReference(@Nullable final SqlNode from)
  {
    if (from instanceof SqlIdentifier) {
      return true;
    }
    return from instanceof SqlCall call && call.getKind() == SqlKind.AS && call.operand(0) instanceof SqlIdentifier;
  }

  private static boolean isStar(final SqlNode node)
  {
    return node instanceof SqlIdentifier identifier && identifier.isStar();
  }

  private Map<String, Object> sourceContext()
  {
    final Map<String, Object> context = new HashMap<>();
    for (final String key : FORWARDED_CONTEXT_KEYS) {
      final Object value = plannerContext.queryContextMap().get(key);
      if (value != null) {
        context.put(key, value);
      }
    }
    // Evaluate time functions and CURRENT_TIMESTAMP on the source as the target would.
    context.put("sqlTimeZone", plannerContext.getTimeZone().getID());
    context.put("sqlCurrentTimestamp", plannerContext.getLocalNow().toString());
    return context;
  }

  /// An external table whose row type is the pushed-down fragment's, so that operators above it are unchanged.
  private static class RemoteDruidSqlTable extends ExternalTable
  {
    private final RelDataType rowType;

    RemoteDruidSqlTable(
        final ExternalDataSource dataSource,
        final RowSignature signature,
        final PlannerContext plannerContext,
        final RelDataType rowType
    )
    {
      super(dataSource, signature, plannerContext.getJsonMapper(), dataSource.getInputSource()::getTypes);
      this.rowType = rowType;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return rowType;
    }
  }
}
