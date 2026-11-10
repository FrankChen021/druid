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

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.msq.exec.QueryKitBasedMSQPlanner;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.logical.DruidLogicalToQueryDefinitionTranslator;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.QueryUtils;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.run.QueryMaker;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Executes Dart queries with up-front planned {@link QueryDefinition}.
 * <p>
 * Normal execution flow utilizes {@code QueryKit} to prdocue the plan;
 * meanwhile it also supports planning the {@link QueryDefinition} directly from
 * the {@link DruidLogicalNode}.
 */
class PrePlannedDartQueryMaker implements QueryMaker, QueryMaker.FromDruidLogical
{
  private final PlannerContext plannerContext;
  private final DartQueryMaker dartQueryMaker;

  public PrePlannedDartQueryMaker(PlannerContext plannerContext, DartQueryMaker queryMaker)
  {
    this.plannerContext = plannerContext;
    this.dartQueryMaker = queryMaker;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidLogicalNode rootRel)
  {
    if (!plannerContext.getAuthorizationResult().allowAccessWithNoRestriction()) {
      throw new ForbiddenException(plannerContext.getAuthorizationResult().getErrorMessage());
    }
    final AtomicBoolean readsSystemTables = new AtomicBoolean();
    DruidLogicalToQueryDefinitionTranslator qdt = new DruidLogicalToQueryDefinitionTranslator(
        plannerContext,
        dataSource -> {
          if (!SystemTableDataSource.readsSystemTable(dataSource)) {
            return dataSource;
          }
          readsSystemTables.set(true);
          return dartQueryMaker.resolveSystemTables(dataSource);
        }
    );
    LogicalStage logicalStage = qdt.translate(rootRel);

    StageMaker maker = new StageMaker(plannerContext);
    maker.buildStage(logicalStage);
    QueryDefinition queryDef = maker.buildQueryDefinition();
    if (readsSystemTables.get() && readsSegments(queryDef)) {
      // The rows of system tables are processed by the Dart worker embedded in the Broker, which can't read segments.
      throw InvalidInput.exception("Dart queries can't read system tables together with other datasources");
    }

    QueryContext context = plannerContext.queryContext();
    ColumnMappings columnMappings = QueryUtils.buildColumnMappings(dartQueryMaker.fieldMapping, logicalStage.getLogicalRowSignature());
    QueryDefMSQSpec querySpec = MSQTaskQueryMaker.buildQueryDefMSQSpec(
        columnMappings,
        plannerContext,
        null,
        queryDef
    );

    ResultsContext resultsContext = MSQTaskQueryMaker.makeSimpleResultContext(
        querySpec.getQueryDef(), rootRel.getRowType(), dartQueryMaker.fieldMapping, plannerContext
    );
    QueryResponse<Object[]> response =
        dartQueryMaker.runQueryDefMSQSpec(querySpec, context, resultsContext, readsSystemTables.get());
    return response;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidQuery druidQuery)
  {
    QueryContext queryContext = druidQuery.getQuery().context();
    ResultsContext resultsContext = DartQueryMaker.makeResultsContext(druidQuery, dartQueryMaker.fieldMapping, plannerContext);
    ColumnMappings columnMappings =
        QueryUtils.buildColumnMappings(dartQueryMaker.fieldMapping, druidQuery.getOutputRowSignature());
    LegacyMSQSpec plannedSpec = MSQTaskQueryMaker.buildLegacyMSQSpec(
        null,
        druidQuery,
        columnMappings,
        plannerContext,
        null
    );
    QueryDefMSQSpec msqSpec = buildMSQSpec(
        dartQueryMaker.resolveSystemTables(plannedSpec),
        columnMappings,
        queryContext,
        resultsContext
    );
    return dartQueryMaker.runQueryDefMSQSpec(
        msqSpec,
        queryContext,
        resultsContext,
        DartQueryMaker.readsSystemTables(plannedSpec)
    );
  }

  private QueryDefMSQSpec buildMSQSpec(
      LegacyMSQSpec querySpec,
      ColumnMappings columnMappings,
      QueryContext queryContext,
      ResultsContext resultsContext)
  {

    final String dartQueryId = queryContext.get(QueryContextParameters.DART_QUERY_ID);

    QueryDefinition queryDef = new QueryKitBasedMSQPlanner(
        querySpec,
        resultsContext,
        querySpec.getQuery(),
        plannerContext.getJsonMapper(),
        dartQueryMaker.queryKitSpecFactory.makeQueryKitSpec(
            dartQueryMaker.queryKit,
            dartQueryId,
            querySpec.getTuningConfig(),
            querySpec.getContext()
        )
    ).makeQueryDefinition();

    return MSQTaskQueryMaker.buildQueryDefMSQSpec(
        columnMappings,
        plannerContext,
        null,
        queryDef
    );
  }

  private static boolean readsSegments(final QueryDefinition queryDef)
  {
    return queryDef.getStageDefinitions()
                   .stream()
                   .flatMap(stage -> stage.getInputSpecs().stream())
                   .anyMatch(TableInputSpec.class::isInstance);
  }
}
