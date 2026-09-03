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

package org.apache.druid.query.context;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Language;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common query context parameter descriptors.
 */
public final class QueryContextParameters
{
  public static final QueryContextParameter<Boolean> FINALIZE = booleanParameter("finalize").build();

  public static final QueryContextParameter<Integer> PRIORITY = integerParameter("priority")
      .defaultValue(QueryContexts.DEFAULT_PRIORITY)
      .build();

  public static final QueryContextParameter<String> LANE = stringParameter("lane").build();

  public static final QueryContextParameter<Long> TIMEOUT = longParameter("timeout").build();

  public static final QueryContextParameter<Long> PER_SEGMENT_TIMEOUT =
      longParameter("perSegmentTimeout").build();

  public static final QueryContextParameter<Long> MAX_SCATTER_GATHER_BYTES =
      longParameter("maxScatterGatherBytes").build();

  public static final QueryContextParameter<Long> MAX_QUEUED_BYTES =
      longParameter("maxQueuedBytes").build();

  public static final QueryContextParameter<Long> DEFAULT_TIMEOUT = longParameter("defaultTimeout")
      .defaultValue(QueryContexts.DEFAULT_TIMEOUT_MILLIS)
      .build();

  public static final QueryContextParameter<Boolean> ENABLE_PARALLEL_MERGE =
      booleanParameter("enableParallelMerge")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_PARALLEL_MERGE)
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_INITIAL_YIELD_ROWS =
      integerParameter("parallelMergeInitialYieldRows").build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_SMALL_BATCH_ROWS =
      integerParameter("parallelMergeSmallBatchRows").build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_PARALLELISM =
      integerParameter("parallelMergeParallelism").build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE =
      enumParameter("vectorize", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE)
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE_VIRTUAL_COLUMNS =
      enumParameter("vectorizeVirtualColumns", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN)
          .build();

  public static final QueryContextParameter<Integer> VECTOR_SIZE = integerParameter("vectorSize")
      .defaultValue(QueryContexts.DEFAULT_VECTOR_SIZE)
      .build();

  public static final QueryContextParameter<Integer> MAX_SUBQUERY_ROWS =
      integerParameter("maxSubqueryRows").build();

  public static final QueryContextParameter<Object> MAX_SUBQUERY_BYTES =
      objectParameter("maxSubqueryBytes").build();

  public static final QueryContextParameter<Boolean> USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY =
      booleanParameter("useNestedForUnknownTypeInSubquery")
          .defaultValue(QueryContexts.DEFAULT_USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_PUSH_DOWN =
      booleanParameter("enableJoinFilterPushDown")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE =
      booleanParameter("enableJoinFilterRewrite")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS =
      booleanParameter("enableJoinFilterRewriteValueColumnFilters")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_REWRITE_JOIN_TO_FILTER =
      booleanParameter("enableRewriteJoinToFilter")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER)
          .build();

  public static final QueryContextParameter<Long> JOIN_FILTER_REWRITE_MAX_SIZE =
      longParameter("joinFilterRewriteMaxSize")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE)
          .build();

  public static final QueryContextParameter<Integer> MAX_NUMERIC_IN_FILTERS =
      integerParameter("maxNumericInFilters").build();

  public static final QueryContextParameter<Boolean> CURSOR_AUTO_ARRANGE_FILTERS =
      booleanParameter("cursorAutoArrangeFilters").build();

  public static final QueryContextParameter<CloneQueryMode> CLONE_QUERY_MODE =
      enumParameter("cloneQueryMode", CloneQueryMode.class)
          .defaultValue(QueryContexts.DEFAULT_CLONE_QUERY_MODE)
          .build();

  public static final QueryContextParameter<Boolean> OPTIMIZE_AGGREGATORS =
      booleanParameter("optimizeAggregators")
          .defaultValue(QueryContexts.DEFAULT_OPTIMIZE_AGGREGATORS)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_LEFT_SCAN_DIRECT =
      booleanParameter("enableJoinLeftTableScanDirect")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT)
          .build();

  public static final QueryContextParameter<Boolean> USE_FILTER_CNF = booleanParameter("useFilterCNF")
      .defaultValue(QueryContexts.DEFAULT_USE_FILTER_CNF)
      .build();

  public static final QueryContextParameter<Integer> NUM_RETRIES_ON_MISSING_SEGMENTS =
      integerParameter("numRetriesOnMissingSegments").build();

  public static final QueryContextParameter<Boolean> RETURN_PARTIAL_RESULTS =
      booleanParameter("returnPartialResults").build();

  public static final QueryContextParameter<Boolean> USE_CACHE = booleanParameter("useCache")
      .defaultValue(QueryContexts.DEFAULT_USE_CACHE)
      .build();

  public static final QueryContextParameter<Boolean> SECONDARY_PARTITION_PRUNING =
      booleanParameter("secondaryPartitionPruning")
          .defaultValue(QueryContexts.DEFAULT_SECONDARY_PARTITION_PRUNING)
          .build();

  public static final QueryContextParameter<Boolean> DEBUG = booleanParameter("debug")
      .defaultValue(QueryContexts.DEFAULT_ENABLE_DEBUG)
      .build();

  public static final QueryContextParameter<Boolean> BY_SEGMENT = booleanParameter("bySegment")
      .defaultValue(QueryContexts.DEFAULT_BY_SEGMENT)
      .build();

  public static final QueryContextParameter<String> BROKER_SERVICE =
      stringParameter("brokerService").build();

  public static final QueryContextParameter<Integer> IN_SUBQUERY_THRESHOLD =
      integerParameter("inSubQueryThreshold")
          .defaultValue(QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD)
          .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_THRESHOLD =
      integerParameter("inFunctionThreshold")
          .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_THRESHOLD)
          .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_EXPR_THRESHOLD =
      integerParameter("inFunctionExprThreshold")
          .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_EXPR_THRESHOLD)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_TIME_BOUNDARY_PLANNING =
      booleanParameter("enableTimeBoundaryPlanning")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING)
          .build();

  public static final QueryContextParameter<Boolean> POPULATE_CACHE =
      booleanParameter("populateCache")
          .defaultValue(QueryContexts.DEFAULT_POPULATE_CACHE)
          .build();

  public static final QueryContextParameter<Boolean> POPULATE_RESULT_LEVEL_CACHE =
      booleanParameter("populateResultLevelCache")
          .defaultValue(QueryContexts.DEFAULT_POPULATE_RESULTLEVEL_CACHE)
          .build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG =
      booleanParameter("serializeDateTimeAsLong").build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG_INNER =
      booleanParameter("serializeDateTimeAsLongInner").build();

  public static final QueryContextParameter<Integer> UNCOVERED_INTERVALS_LIMIT =
      integerParameter("uncoveredIntervalsLimit")
          .defaultValue(QueryContexts.DEFAULT_UNCOVERED_INTERVALS_LIMIT)
          .build();

  public static final QueryContextParameter<Integer> MIN_TOP_N_THRESHOLD =
      integerParameter("minTopNThreshold").build();

  public static final QueryContextParameter<Boolean> CATALOG_VALIDATION_ENABLED =
      booleanParameter("catalogValidationEnabled")
          .defaultValue(QueryContexts.DEFAULT_CATALOG_VALIDATION_ENABLED)
          .build();

  public static final QueryContextParameter<String> ENGINE = stringParameter("engine")
      .defaultValue(QueryContexts.DEFAULT_ENGINE)
      .build();

  public static final QueryContextParameter<Boolean> USE_TOPN_MULTI_PASS_POOLED_QUERY_GRANULARITY =
      booleanParameter("useTopNMultiPassPooledQueryGranularity").build();

  public static final QueryContextParameter<Boolean> USE_RESULT_LEVEL_CACHE = booleanParameter("useResultLevelCache")
      .defaultValue(true)
      .docs(
          doc().description(
                   """
                   Flag indicating whether to leverage the result level cache for this query. When set to false, it \
                   disables reading from the query cache for this query. When set to true, Druid uses \
                   `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the \
                   result-level query cache.\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Boolean> EXTENDED_FILTERED_SUM_REWRITE =
      booleanParameter("extendedFilteredSumRewrite")
          .defaultValue(QueryContexts.DEFAULT_EXTENDED_FILTERED_SUM_REWRITE_ENABLED)
          .build();

  public static final QueryContextParameter<Boolean> NO_PROJECTIONS =
      booleanParameter("noProjections").build();

  public static final QueryContextParameter<Boolean> FORCE_PROJECTIONS =
      booleanParameter("forceProjections").build();

  public static final QueryContextParameter<String> USE_PROJECTION =
      stringParameter("useProjection").build();

  public static final QueryContextParameter<String> QUERY_RESOURCE_ID =
      stringParameter("queryResourceId").build();

  public static final QueryContextParameter<String> SQL_QUERY_ID =
      stringParameter("sqlQueryId").build();

  public static final QueryContextParameter<Boolean> SQL_STRINGIFY_ARRAYS =
      booleanParameter("sqlStringifyArrays").build();

  public static final QueryContextParameter<String> DART_QUERY_ID =
      stringParameter("dartQueryId").build();

  public static final QueryContextParameter<Boolean> FULL_REPORT = booleanParameter("fullReport")
      .defaultValue(QueryContexts.DEFAULT_CTX_FULL_REPORT)
      .build();

  public static final QueryContextParameter<ExecutionMode> EXECUTION_MODE =
      enumParameter("executionMode", ExecutionMode.class).build();

  public static final QueryContextParameter<String> NATIVE_QUERY_SQL_PLANNING_MODE =
      stringParameter("plannerStrategy")
          .defaultValue(QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED)
          .build();

  public static final QueryContextParameter<Boolean> REALTIME_SEGMENTS_ONLY =
      booleanParameter("realtimeSegmentsOnly")
          .defaultValue(false)
          .deprecated("Use realtimeSegmentsMode instead.")
          .build();

  public static final QueryContextParameter<QueryContexts.RealtimeSegmentsMode> REALTIME_SEGMENTS_MODE =
      enumParameter("realtimeSegmentsMode", QueryContexts.RealtimeSegmentsMode.class)
          .defaultValue(QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE)
          .build();

  public static final QueryContextParameter<Boolean> PREPLANNED = booleanParameter("prePlanned")
      .defaultValue(QueryContexts.DEFAULT_PREPLANNED)
      .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_QUEUED_FOR_ORDERING =
      integerParameter("maxRowsQueuedForOrdering")
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .docs(
              doc().description(
                       """
                       The maximum number of rows returned when time ordering is used. Overrides the identically \
                       named config.\
                       """
                   )
                   .defaultDescription("druid.query.scan.maxRowsQueuedForOrdering")
                   .language(Language.NATIVE)
                   .engine(Engine.NATIVE)
                   .query(QueryType.SCAN)
                   .build()
          )
          .build();

  /** Immutable query context parameter descriptors indexed by parameter name. */
  public static final Map<String, QueryContextParameter<?>> BY_NAME =
      Arrays.stream(QueryContextParameters.class.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers()))
            .filter(field -> QueryContextParameter.class.equals(field.getType()))
            .map(QueryContextParameters::getParameter)
            .collect(Collectors.toUnmodifiableMap(QueryContextParameter::getName, Function.identity()));

  private QueryContextParameters()
  {
  }

  /**
   * Validates a value assigned by a SQL {@code SET} statement.
   */
  public static void validate(final String name, @Nullable final Object value)
  {
    final QueryContextParameter<?> parameter = BY_NAME.get(name);
    // Unmigrated parameters are intentionally accepted until the catalog contains every supported context parameter.
    if (parameter != null) {
      parameter.parse(value);
    }
  }

  /**
   * Validates every recognized query context parameter in the supplied map.
   */
  public static void validate(final Map<String, Object> parameters)
  {
    parameters.forEach(QueryContextParameters::validate);
  }

  private static QueryContextParameter<?> getParameter(final Field field)
  {
    try {
      return (QueryContextParameter<?>) field.get(null);
    }
    catch (final IllegalAccessException e) {
      throw new ISE(e, "Unable to read query context parameter field [%s]", field.getName());
    }
  }

  // These builders temporarily delegate to QueryContexts for its established coercion behavior. The coercion logic
  // can move into this class after all query context parameters and their callers have migrated to descriptors.
  static QueryContextParameter.Builder<Boolean> booleanParameter(final String name)
  {
    return QueryContextParameter.builder(name, Boolean.class, value -> QueryContexts.getAsBoolean(name, value));
  }

  static QueryContextParameter.Builder<Integer> integerParameter(final String name)
  {
    return QueryContextParameter.builder(name, Integer.class, value -> QueryContexts.getAsInt(name, value));
  }

  static QueryContextParameter.Builder<Long> longParameter(final String name)
  {
    return QueryContextParameter.builder(name, Long.class, value -> QueryContexts.getAsLong(name, value));
  }

  static QueryContextParameter.Builder<String> stringParameter(final String name)
  {
    return QueryContextParameter.builder(name, String.class, value -> QueryContexts.getAsString(name, value, null));
  }

  static <T extends Enum<T>> QueryContextParameter.Builder<T> enumParameter(
      final String name,
      final Class<T> valueType
  )
  {
    return QueryContextParameter.builder(name, valueType, value -> QueryContexts.getAsEnum(name, value, valueType));
  }

  static QueryContextParameter.Builder<Object> objectParameter(final String name)
  {
    return QueryContextParameter.builder(name, Object.class, value -> value);
  }

  private static ParameterDocumentation.Builder doc()
  {
    return ParameterDocumentation.builder();
  }
}
