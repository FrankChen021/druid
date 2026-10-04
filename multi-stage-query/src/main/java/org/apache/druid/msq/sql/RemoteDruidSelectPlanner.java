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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.data.input.impl.RemoteDruidFrameInputSource;
import org.apache.druid.data.input.impl.RemoteDruidInputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalTableScan;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.RelParameterizerShuttle;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.logical.DruidSort;
import org.apache.druid.sql.calcite.table.ExternalTable;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/// Converts a supported remote Druid relational fragment to source SQL and a target frame scan.
public final class RemoteDruidSelectPlanner
{
  private static final Set<String> SOURCE_CONTEXT_KEYS = Set.of(
      "sqlTimeZone",
      "sqlCurrentTimestamp",
      "sqlStringifyArrays",
      "sqlUseApproximateCountDistinct",
      "sqlUseFallback",
      "timeout",
      "priority"
  );

  private RemoteDruidSelectPlanner()
  {
  }

  /// Detects remote table inputs before native lowering, including inputs inside unsupported joins or unions.
  public static boolean containsRemoteDruidTable(final RelNode root)
  {
    final boolean[] found = {false};
    new RelVisitor()
    {
      @Override
      public void visit(final RelNode node, final int ordinal, @Nullable final RelNode parent)
      {
        if (node instanceof ExternalTableScan scan
            && scan.getDruidTable().getDataSource() instanceof ExternalDataSource external
            && external.getInputSource() instanceof RemoteDruidInputSource) {
          found[0] = true;
        }
        for (final RexNode expression : expressions(node)) {
          expression.accept(new RexVisitorImpl<Void>(true)
          {
            @Override
            public Void visitSubQuery(final RexSubQuery subQuery)
            {
              found[0] |= containsRemoteDruidTable(subQuery.rel);
              return super.visitSubQuery(subQuery);
            }
          });
        }
        super.visit(node, ordinal, parent);
      }
    }.go(root);
    return found[0];
  }

  /// Creates a target query that scans one source-side SELECT result without repeating its operators.
  public static DruidQuery plan(final RelRoot relRoot, final PlannerContext plannerContext)
  {
    final RelNode parameterized = relRoot.rel.accept(new RelParameterizerShuttle(plannerContext));
    final RelNode projected = relRoot.withRel(parameterized).project();
    final boolean targetClustering = hasTargetClustering(plannerContext);
    final RelNode sourceRoot = targetClustering ? removeOutputSort(projected) : projected;
    final ScanInfo scanInfo = validateAndFindRemoteScan(sourceRoot, relRoot, targetClustering);
    final RemoteDruidInputSource originalInputSource = scanInfo.inputSource;

    final RowSignature outputSignature = outputSignature(sourceRoot);
    validateSupportedSignature(outputSignature, "Remote SELECT output");
    final String sourceSql = toSourceSql(sourceRoot, plannerContext);
    final Map<String, Object> sourceContext = sourceContext(plannerContext);
    final RemoteDruidFrameInputSource frameInputSource = originalInputSource.asFrameInputSource(
        sourceSql,
        requestId(plannerContext.getSqlQueryId(), sourceSql, sourceContext),
        sourceContext,
        outputSignature
    );

    final ExternalDataSource targetDataSource = new ExternalDataSource(frameInputSource, null, outputSignature);
    final ExternalTable targetTable = new ExternalTable(
        targetDataSource,
        outputSignature,
        plannerContext.getJsonMapper(),
        frameInputSource::getTypes
    );
    final ExternalTableScan targetScan = new ExternalTableScan(
        projected.getCluster(),
        plannerContext.getJsonMapper(),
        targetTable
    );
    DruidQueryRel targetQueryRel = DruidQueryRel.scanExternal(targetScan, plannerContext);
    if (targetClustering && !relRoot.collation.getFieldCollations().isEmpty()) {
      final Sort targetSort = DruidSort.create(targetScan, relRoot.collation, null, null);
      targetQueryRel = targetQueryRel.withPartialQuery(targetQueryRel.getPartialDruidQuery().withSort(targetSort));
    }
    return targetQueryRel.toDruidQuery(true);
  }

  private static ScanInfo validateAndFindRemoteScan(
      final RelNode root,
      final RelRoot relRoot,
      final boolean targetClustering
  )
  {
    if (!targetClustering && !relRoot.collation.getFieldCollations().isEmpty()) {
      throw InvalidInput.exception("Remote Druid MSQ does not support ORDER BY");
    }

    final List<ExternalTableScan> scans = new ArrayList<>();
    new RelVisitor()
    {
      @Override
      public void visit(final RelNode node, final int ordinal, @Nullable final RelNode parent)
      {
        if (node instanceof Sort || node instanceof Window) {
          throw InvalidInput.exception("Remote Druid MSQ does not support ORDER BY, LIMIT, or window functions");
        }
        if (!(node instanceof ExternalTableScan || node instanceof Project || node instanceof Filter
              || node instanceof Aggregate || node instanceof Calc)) {
          throw InvalidInput.exception(
              "Remote Druid MSQ supports one remote Druid table with projection, filters, and aggregation"
          );
        }
        for (final RexNode expression : expressions(node)) {
          expression.accept(new RexVisitorImpl<Void>(true)
          {
            @Override
            public Void visitOver(final RexOver over)
            {
              throw InvalidInput.exception("Remote Druid MSQ does not support window functions");
            }

            @Override
            public Void visitSubQuery(final RexSubQuery subQuery)
            {
              throw InvalidInput.exception("Remote Druid MSQ does not support relational subqueries");
            }
          });
        }
        if (node instanceof ExternalTableScan) {
          scans.add((ExternalTableScan) node);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(root);

    if (scans.size() != 1) {
      throw InvalidInput.exception("Remote Druid MSQ requires exactly one remote Druid table");
    }
    if (!(scans.get(0).getDruidTable().getDataSource() instanceof ExternalDataSource)) {
      throw InvalidInput.exception("Remote Druid MSQ requires a TABLE(DRUID(...)) input");
    }
    final ExternalDataSource externalDataSource =
        (ExternalDataSource) scans.get(0).getDruidTable().getDataSource();
    if (!(externalDataSource.getInputSource() instanceof RemoteDruidInputSource)) {
      throw InvalidInput.exception("Remote Druid MSQ requires a TABLE(DRUID(...)) input");
    }
    return new ScanInfo((RemoteDruidInputSource) externalDataSource.getInputSource());
  }

  private static boolean hasTargetClustering(final PlannerContext plannerContext)
  {
    if (!(plannerContext.getSqlNode() instanceof DruidSqlIngest)) {
      return false;
    }

    final DruidSqlIngest ingest = (DruidSqlIngest) plannerContext.getSqlNode();
    final SqlNodeList clusteredBy = ingest.getClusteredBy();
    if (clusteredBy != null && !clusteredBy.getList().isEmpty()) {
      return true;
    }

    SqlNode source = ingest.getSource();
    while (source instanceof SqlWith) {
      source = ((SqlWith) source).body;
    }
    if (source instanceof SqlSelect) {
      final SqlNodeList orderBy = ((SqlSelect) source).getOrderList();
      return orderBy != null && !orderBy.getList().isEmpty();
    }
    return false;
  }

  private static RelNode removeOutputSort(final RelNode root)
  {
    if (root instanceof Sort) {
      final Sort sort = (Sort) root;
      validateNoLimit(sort);
      return sort.getInput();
    }
    if (root instanceof Project) {
      final Project project = (Project) root;
      if (project.isMapping() && project.getInput() instanceof Sort) {
        final Sort sort = (Sort) project.getInput();
        validateNoLimit(sort);
        return project.copy(project.getTraitSet(), sort.getInput(), project.getProjects(), project.getRowType());
      }
    }
    return root;
  }

  private static void validateNoLimit(final Sort sort)
  {
    if (sort.fetch != null || sort.offset != null) {
      throw InvalidInput.exception("Remote Druid MSQ does not support LIMIT or OFFSET");
    }
  }

  private static List<RexNode> expressions(final RelNode node)
  {
    if (node instanceof Project) {
      return ((Project) node).getProjects();
    }
    if (node instanceof Filter) {
      return ImmutableList.of(((Filter) node).getCondition());
    }
    if (node instanceof Calc) {
      return ((Calc) node).getProgram().getExprList();
    }
    return ImmutableList.of();
  }

  private static String toSourceSql(final RelNode root, final PlannerContext plannerContext)
  {
    final RelNode sourceRoot = root.accept(new RelShuttleImpl()
    {
      @Override
      public RelNode visit(final RelNode node)
      {
        if (node instanceof ExternalTableScan) {
          return sourceRelation((ExternalTableScan) node, plannerContext);
        }
        return super.visit(node);
      }
    });

    final SqlNode sqlNode = new RelToSqlConverter(CalciteSqlDialect.DEFAULT).visitRoot(sourceRoot).asStatement();
    return sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
  }

  private static RelNode sourceRelation(final ExternalTableScan externalScan, final PlannerContext plannerContext)
  {
    final ExternalDataSource externalDataSource = (ExternalDataSource) externalScan.getDruidTable().getDataSource();
    final RemoteDruidInputSource remoteInputSource = (RemoteDruidInputSource) externalDataSource.getInputSource();
    final RelDataTypeFactory typeFactory = externalScan.getCluster().getTypeFactory();
    final RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    final RelDataTypeFactory.Builder physicalRowTypeBuilder = typeFactory.builder();
    for (final String columnName : externalScan.getRowType().getFieldNames()) {
      physicalRowTypeBuilder.add(columnName, anyType);
    }
    final RelDataType physicalRowType = physicalRowTypeBuilder.build();
    final Table calciteTable = new AbstractTable()
    {
      @Override
      public RelDataType getRowType(final RelDataTypeFactory typeFactory)
      {
        return physicalRowType;
      }
    };
    final RelOptTable relOptTable = RelOptTableImpl.create(
        null,
        physicalRowType,
        ImmutableList.of(remoteInputSource.getDataSource()),
        Expressions.constant(calciteTable)
    );
    final LogicalTableScan tableScan = LogicalTableScan.create(
        externalScan.getCluster(),
        relOptTable,
        ImmutableList.of()
    );

    final RelDataType targetRowType = externalScan.getRowType();
    final RexBuilder rexBuilder = externalScan.getCluster().getRexBuilder();
    final List<RexNode> projects = new ArrayList<>(targetRowType.getFieldCount());
    final List<String> names = new ArrayList<>(targetRowType.getFieldCount());
    for (int i = 0; i < targetRowType.getFieldCount(); i++) {
      final String columnName = targetRowType.getFieldNames().get(i);
      final RelDataType physicalType = physicalRowType.getFieldList().get(i).getType();
      final RelDataType targetType = targetRowType.getFieldList().get(i).getType();
      final RexNode inputRef = rexBuilder.makeInputRef(physicalType, i);
      projects.add(rexBuilder.makeCast(targetType, inputRef));
      names.add(columnName);
    }

    RelNode sourceRelation = LogicalProject.create(tableScan, ImmutableList.of(), projects, names);
    final List<Interval> intervals = remoteInputSource.getIntervals();
    if (intervals != null) {
      final int timeColumn = targetRowType.getFieldNames().indexOf(ColumnHolder.TIME_COLUMN_NAME);
      if (timeColumn < 0) {
        throw InvalidInput.exception("Remote Druid source schema must include __time");
      }
      final RexNode timeRef = rexBuilder.makeInputRef(sourceRelation, timeColumn);
      final List<RexNode> ranges = new ArrayList<>(intervals.size());
      final RelDataType timeType = targetRowType.getFieldList().get(timeColumn).getType();
      final boolean timestampTimeColumn = timeType.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
                                          || timeType.getSqlTypeName()
                                             == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      if (!timestampTimeColumn && !SqlTypeUtil.isNumeric(timeType)) {
        throw InvalidInput.exception("Remote Druid source __time must be a timestamp or numeric column");
      }
      final DateTimeZone timeZone = plannerContext.getTimeZone();
      for (final Interval interval : intervals) {
        if (Intervals.ETERNITY.equals(interval)) {
          return sourceRelation;
        }
        final RexNode start = timestampTimeColumn
                              ? rexBuilder.makeTimestampLiteral(intervalTimestamp(interval.getStartMillis(), timeZone), 3)
                              : rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(interval.getStartMillis()));
        final RexNode end = timestampTimeColumn
                            ? rexBuilder.makeTimestampLiteral(intervalTimestamp(interval.getEndMillis(), timeZone), 3)
                            : rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(interval.getEndMillis()));
        ranges.add(rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, timeRef, start),
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, timeRef, end)
        ));
      }
      final RexNode condition = ranges.isEmpty()
                               ? rexBuilder.makeLiteral(false)
                               : ranges.size() == 1 ? ranges.get(0)
                                                    : rexBuilder.makeCall(SqlStdOperatorTable.OR, ranges);
      sourceRelation = LogicalFilter.create(sourceRelation, condition);
    }
    return sourceRelation;
  }

  private static TimestampString intervalTimestamp(final long millis, final DateTimeZone timeZone)
  {
    return new TimestampString(new DateTime(millis, timeZone).toString("yyyy-MM-dd HH:mm:ss.SSS"));
  }

  private static RowSignature outputSignature(final RelNode root)
  {
    final RowSignature signature = RowSignatures.fromRelDataType(root.getRowType().getFieldNames(), root.getRowType());
    validateSupportedSignature(signature, "Remote SELECT output");
    return signature;
  }

  private static void validateSupportedSignature(final RowSignature signature, final String description)
  {
    for (final String column : signature.getColumnNames()) {
      final ColumnType type = signature.getColumnType(column).orElse(null);
      if (type == null || !(type.isPrimitive() || type.isPrimitiveArray())) {
        throw InvalidInput.exception("%s contains an unsupported type for column[%s]", description, column);
      }
    }
  }

  private static Map<String, Object> sourceContext(final PlannerContext plannerContext)
  {
    final Map<String, Object> sourceContext = new HashMap<>();
    for (final String key : SOURCE_CONTEXT_KEYS) {
      if (plannerContext.queryContextMap().containsKey(key)) {
        sourceContext.put(key, plannerContext.queryContextMap().get(key));
      }
    }
    sourceContext.put("sqlTimeZone", plannerContext.getTimeZone().getID());
    sourceContext.put("sqlCurrentTimestamp", plannerContext.getLocalNow().toString());
    return Map.copyOf(sourceContext);
  }

  private static String requestId(
      final String queryId,
      final String sourceSql,
      final Map<String, Object> sourceContext
  )
  {
    return Hashing.sha256()
                 .hashString(queryId + "\u0000" + sourceSql + "\u0000" + new TreeMap<>(sourceContext), StandardCharsets.UTF_8)
                 .toString();
  }

  private static class ScanInfo
  {
    private final RemoteDruidInputSource inputSource;

    private ScanInfo(final RemoteDruidInputSource inputSource)
    {
      this.inputSource = inputSource;
    }
  }
}
