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
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig;
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
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.data.input.impl.RemoteDruidSqlInputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringToArrayOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimestampToMillisOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.logical.DruidJoinRule;
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
  /// Query context keys copied as given to the source.
  private static final List<String> FORWARDED_CONTEXT_KEYS = List.of(QueryContexts.TIMEOUT_KEY, QueryContexts.PRIORITY_KEY);

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
    // Move filters above joins with a remote input into the join inputs, so that the remote input can absorb them.
    // Joins of local inputs are left alone, as are join conditions that Druid cannot evaluate.
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addMatchLimit(CalciteRulesManager.HEP_DEFAULT_MATCH_LIMIT);
    builder.addRuleInstance(
        FilterIntoJoinRuleConfig.DEFAULT
            .withOperandSupplier(
                b0 -> b0.operand(Filter.class)
                        .oneInput(b1 -> b1.operand(Join.class)
                                          .predicate(RemoteDruidPushdownProgram::hasRemoteInput)
                                          .anyInputs())
            )
            .as(FilterIntoJoinRuleConfig.class)
            .withPredicate(DruidJoinRule::isSupportedPredicate)
            .toRule()
    );
    builder.addRuleInstance(
        JoinConditionPushRuleConfig.DEFAULT
            .withOperandSupplier(
                b0 -> b0.operand(Join.class).predicate(RemoteDruidPushdownProgram::hasRemoteInput).anyInputs()
            )
            .as(JoinConditionPushRuleConfig.class)
            .withPredicate(DruidJoinRule::isSupportedPredicate)
            .toRule()
    );
    final RelNode pushed = Programs.of(builder.build(), true, DefaultRelMetadataProvider.INSTANCE)
                                   .run(planner, rel, requiredOutputTraits, materializations, lattices);
    return rewrite(pushed);
  }

  private static boolean hasRemoteInput(final Join join)
  {
    return containsRemoteScan(join.getLeft()) || containsRemoteScan(join.getRight());
  }

  private static boolean containsRemoteScan(final RelNode node)
  {
    final RelNode rel = node.stripped();
    if (isRemoteScan(rel)) {
      return true;
    }
    for (final RelNode input : rel.getInputs()) {
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
        // Planner-generated aggregates, such as SINGLE_VALUE for scalar subqueries, may not exist in older sources.
        if (call.getAggregation().getKind() == SqlKind.SINGLE_VALUE || call.getAggregation().getKind() == SqlKind.SUM0) {
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
    // Grouping returns one row per value of a multi-value string, so only plain reads return multi-value rows.
    final String sql = toSql(timeRangeParameters ? withMultiValueArrays(source) : source, remoteScan.getRowType());

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
    final RelNode scan = withDeclaredNumericTypes(
        LogicalTableScan.create(remoteScan.getCluster(), table, ImmutableList.of())
    );

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

  /// Compares `__time` in epoch milliseconds. Comparing with timestamps would convert the bounds through the session
  /// time zone, which is ambiguous when clocks are set back.
  private static RexNode timeRange(
      final RexBuilder rexBuilder,
      final RexNode time,
      final RexNode startMillis,
      final RexNode endMillis
  )
  {
    final RexNode millis = rexBuilder.makeCall(new TimestampToMillisOperatorConversion().calciteOperator(), time);
    return rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, millis, startMillis),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, millis, endMillis)
    );
  }

  /// Casts declared numeric columns, so that the source evaluates pushed-down operators with the declared types even
  /// if its own column types differ. `__time` and string columns are used as they are on the source.
  private static RelNode withDeclaredNumericTypes(final RelNode scan)
  {
    final RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
    final List<RexNode> projects = new ArrayList<>();
    boolean cast = false;
    for (final RelDataTypeField field : scan.getRowType().getFieldList()) {
      final RexNode column = rexBuilder.makeInputRef(scan, field.getIndex());
      if (SqlTypeUtil.isNumeric(field.getType()) && !ColumnHolder.TIME_COLUMN_NAME.equals(field.getName())) {
        // An abstract cast is kept even though the column already has this type in the plan.
        projects.add(rexBuilder.makeAbstractCast(field.getType(), column, false));
        cast = true;
      } else {
        projects.add(column);
      }
    }
    return cast ? LogicalProject.create(scan, List.of(), projects, scan.getRowType().getFieldNames()) : scan;
  }

  /// Wraps string outputs that are source columns, unchanged, in `MV_TO_ARRAY`. Druid SQL returns multi-value strings
  /// as JSON-encoded text, while arrays arrive as JSON arrays that the reader turns back into multi-value values.
  /// `MV_TO_ARRAY` only accepts columns, so computed strings are returned as they are.
  private static RelNode withMultiValueArrays(final RelNode source)
  {
    final RelMetadataQuery metadataQuery = source.getCluster().getMetadataQuery();
    final RexBuilder rexBuilder = source.getCluster().getRexBuilder();
    final List<RexNode> projects = new ArrayList<>();
    boolean wrapped = false;
    for (final RelDataTypeField field : source.getRowType().getFieldList()) {
      final RexNode column = rexBuilder.makeInputRef(source, field.getIndex());
      final Set<RelColumnOrigin> origins = metadataQuery.getColumnOrigins(source, field.getIndex());
      if (SqlTypeUtil.isCharacter(field.getType())
          && origins != null
          && origins.size() == 1
          && !origins.iterator().next().isDerived()) {
        projects.add(rexBuilder.makeCall(MultiValueStringToArrayOperatorConversion.SQL_FUNCTION, column));
        wrapped = true;
      } else {
        projects.add(column);
      }
    }
    return wrapped ? LogicalProject.create(source, List.of(), projects, source.getRowType().getFieldNames()) : source;
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
    // Plan aggregations as the target would, using its effective settings rather than only those in the context.
    final PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, plannerConfig.isUseApproximateCountDistinct());
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, plannerConfig.isUseApproximateTopN());
    context.put(
        PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
        plannerConfig.isUseGroupingSetForExactDistinct()
    );
    // Evaluate time functions and CURRENT_TIMESTAMP on the source as the target would.
    context.put(PlannerContext.CTX_SQL_TIME_ZONE, plannerContext.getTimeZone().getID());
    context.put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, plannerContext.getLocalNow().toString());
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
