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
  public static final QueryContextParameter<Boolean> FINALIZE = booleanParameter("finalize")
      .docs(
          doc().description(
                   """
                   Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For \
                   instance, the `hyperUnique` aggregator returns the full HyperLogLog sketch instead of the estimated \
                   cardinality when this flag is set to `false`.\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Integer> PRIORITY = integerParameter("priority")
      .defaultValue(QueryContexts.DEFAULT_PRIORITY)
      .docs(
          doc().description("Query priority. Queries with higher priority get precedence for computational resources.")
               .defaultDescription(
                   """
                   The default priority is one of the following: <ul><li>Value of `priority` in the query context, if \
                   set</li><li>The value of the runtime property `druid.query.default.context.priority`, if set and \
                   not null</li><li>`0` if the priority is not set in the query context or runtime properties</li></ul>\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<String> LANE = stringParameter("lane")
      .docs(
          doc().description(
                   """
                   Query lane, used to control usage limits on classes of queries. See [Broker configuration](../\
                   configuration/index.md#broker) for more details.\
                   """
               )
               .defaultDescription("`null`")
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Long> TIMEOUT = longParameter("timeout")
      .docs(
          doc().description(
                   """
                   Query timeout in millis, beyond which unfinished queries will be cancelled. 0 timeout means `no \
                   timeout` (up to the server-side maximum query timeout, `druid.server.http.maxQueryTimeout`). To set \
                   the default timeout and maximum timeout, see [Broker configuration](../configuration/index.md#broker).\
                   """
               )
               .defaultDescription("`druid.server.http.defaultQueryTimeout`")
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Long> PER_SEGMENT_TIMEOUT =
      longParameter("perSegmentTimeout")
          .docs(
              doc().description(
                       """
                       Per-segment processing timeout in millis, beyond which unfinished queries will be cancelled. \
                       Should be ≤ `timeout`. 0 `perSegmentTimeout` means `no per-segment timeout`. Generally, a standard \
                       default should be O(X seconds). A cluster-wide default value for this query context can be specified \
                       via `druid.query.default.context.perSegmentTimeout`.\
                       """
                   )
                   .defaultDescription("`null`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Long> MAX_SCATTER_GATHER_BYTES =
      longParameter("maxScatterGatherBytes")
          .docs(
              doc().description(
                       """
                       Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute \
                       a query. This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time. See \
                       [Broker configuration](../configuration/index.md#broker) for more details.\
                       """
                   )
                   .defaultDescription("`druid.server.http.maxScatterGatherBytes`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Long> MAX_QUEUED_BYTES =
      longParameter("maxQueuedBytes")
          .docs(
              doc().description(
                       """
                       Maximum number of bytes queued per query before exerting backpressure on the channel to the data server. Similar \
                       to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than \
                       query failure. Zero means disabled.\
                       """
                   )
                   .defaultDescription("`druid.broker.http.maxQueuedBytes`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Long> DEFAULT_TIMEOUT = longParameter("defaultTimeout")
      .defaultValue(QueryContexts.DEFAULT_TIMEOUT_MILLIS)
      .build();

  public static final QueryContextParameter<Boolean> ENABLE_PARALLEL_MERGE =
      booleanParameter("enableParallelMerge")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_PARALLEL_MERGE)
          .docs(
              doc().description(
                       """
                       Enable parallel result merging on the Broker. Note that `druid.processing.merge.useParallelMergePool` must \
                       be enabled for this setting to be set to `true`. See [Broker configuration](../configuration/index.md#broker) \
                       for more details.\
                       """
                   )
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_INITIAL_YIELD_ROWS =
      integerParameter("parallelMergeInitialYieldRows")
          .docs(
              doc().description(
                       """
                       Number of rows to yield per ForkJoinPool merge task for parallel result merging on the Broker, before forking off \
                       a new task to continue merging sequences. See [Broker configuration](../configuration/index.md#broker) for more \
                       details.\
                       """
                   )
                   .defaultDescription("`druid.processing.merge.initialYieldNumRows`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_SMALL_BATCH_ROWS =
      integerParameter("parallelMergeSmallBatchRows")
          .docs(
              doc().description(
                       """
                       Size of result batches to operate on in ForkJoinPool merge tasks for parallel result merging on the Broker. See \
                       [Broker configuration](../configuration/index.md#broker) for more details.\
                       """
                   )
                   .defaultDescription("`druid.processing.merge.smallBatchNumRows`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_PARALLELISM =
      integerParameter("parallelMergeParallelism")
          .docs(
              doc().description(
                       """
                       Maximum number of parallel threads to use for parallel result merging on the Broker. See [Broker configuration](../\
                       configuration/index.md#broker) for more details.\
                       """
                   )
                   .defaultDescription("`druid.processing.merge.parallelism`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE =
      enumParameter("vectorize", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE)
          .docs(
              doc().description(
                       """
                       Enables or disables vectorized query execution. Possible values are `false` (disabled), `true` \
                       (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and query \
                       types that support vectorization will fail if they cannot be vectorized). The `"force"` setting is \
                       meant to aid in testing, and is not generally useful in production (since real-time segments can never \
                       be processed with vectorized execution, any queries on real-time data will fail). This will override \
                       `druid.query.default.context.vectorize` if it's set.\
                       """
                   )
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE_VIRTUAL_COLUMNS =
      enumParameter("vectorizeVirtualColumns", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN)
          .docs(
              doc().description(
                       """
                       Enables or disables vectorized query processing of queries with virtual columns, layered on top of \
                       `vectorize` (`vectorize` must also be set to true for a query to utilize vectorization). Possible \
                       values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), \
                       and `force` (enabled, and groupBy or timeseries queries with virtual columns that cannot be vectorized \
                       will fail). The `"force"` setting is meant to aid in testing, and is not generally useful in production. \
                       This will override `druid.query.default.context.vectorizeVirtualColumns` if it's set.\
                       """
                   )
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> VECTOR_SIZE = integerParameter("vectorSize")
      .defaultValue(QueryContexts.DEFAULT_VECTOR_SIZE)
      .docs(
          doc().description("Sets the row batching size for a particular query. This will override `druid.query.default.context.vectorSize` if it's set.")
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Integer> MAX_SUBQUERY_ROWS =
      integerParameter("maxSubqueryRows")
          .docs(
              doc().description(
                       """
                       Upper limit on the number of rows a subquery can generate. See [Broker configuration](../\
                       configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for \
                       materialization of subqueries) for more details.\
                       """
                   )
                   .defaultDescription("`druid.server.http.maxSubqueryRows`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Object> MAX_SUBQUERY_BYTES =
      objectParameter("maxSubqueryBytes")
          .docs(
              doc().description(
                       """
                       Upper limit on the number of bytes a subquery can generate. See [Broker configuration](../\
                       configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for \
                       materialization of subqueries) for more details.\
                       """
                   )
                   .defaultDescription("`druid.server.http.maxSubqueryBytes`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY =
      booleanParameter("useNestedForUnknownTypeInSubquery")
          .defaultValue(QueryContexts.DEFAULT_USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_PUSH_DOWN =
      booleanParameter("enableJoinFilterPushDown")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN)
          .docs(
              doc().description(
                       "Controls whether a join query will attempt filter push down, which reduces the number of rows that have to be compared in a join operation.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE =
      booleanParameter("enableJoinFilterRewrite")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE)
          .docs(
              doc().description(
                       "Controls whether filter clauses that reference non-base table columns will be rewritten into filters on base table columns.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS =
      booleanParameter("enableJoinFilterRewriteValueColumnFilters")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS)
          .docs(
              doc().description(
                       "Controls whether Druid rewrites non-base table filters on non-key columns in the non-base table. Requires a scan of the non-base table.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_REWRITE_JOIN_TO_FILTER =
      booleanParameter("enableRewriteJoinToFilter")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER)
          .docs(
              doc().description(
                       "Controls whether a join can be pushed partial or fully to the base table as a filter at runtime.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Long> JOIN_FILTER_REWRITE_MAX_SIZE =
      longParameter("joinFilterRewriteMaxSize")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE)
          .docs(
              doc().description(
                       "The maximum size of the correlated value set used for filter rewrites. Set this limit to prevent excessive memory use.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> MAX_NUMERIC_IN_FILTERS =
      integerParameter("maxNumericInFilters")
          .docs(
              doc().description(
                       """
                       Max limit for the amount of numeric values that Druid can compare for a string type dimension \
                       when the entire SQL WHERE clause of a query translates only to an [OR](../querying/filters.md#or) of \
                       [bound filter](../querying/filters.md#bound-filter). By default, Druid doesn't restrict the amount \
                       of numeric bound filters on string columns, although this situation may block other queries from running. \
                       Set this parameter to a smaller value to prevent Druid from running queries that have prohibitively long \
                       segment processing times. The optimal limit requires some trial and error. We recommend starting with 100. \
                       Users who submit a query that exceeds the limit of `maxNumericInFilters` should rewrite their queries to use \
                       strings in the `WHERE` clause instead of numbers. For example, `WHERE someString IN (‘123’, ‘456’)`. \
                       This value can't exceed the set system configuration `druid.sql.planner.maxNumericInFilters`. If \
                       `druid.sql.planner.maxNumericInFilters` isn't set explicitly, Druid ignores this value.\
                       """
                   )
                   .defaultDescription("`-1`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> CURSOR_AUTO_ARRANGE_FILTERS =
      booleanParameter("cursorAutoArrangeFilters").build();

  public static final QueryContextParameter<CloneQueryMode> CLONE_QUERY_MODE =
     enumParameter("cloneQueryMode", CloneQueryMode.class)
         .defaultValue(QueryContexts.DEFAULT_CLONE_QUERY_MODE)
          .docs(
              doc().description(
                       """
                       Indicates whether clone Historicals should be queried by brokers. Clone servers are created by \
                       the `cloneServers` Coordinator dynamic configuration. Possible values are `excludeClones`, \
                       `includeClones` and `preferClones`. `excludeClones` means that clone Historicals are not queried \
                       by the broker. `preferClones` indicates that when given a choice between the clone Historical and \
                       the original Historical which is being cloned, the broker chooses the clones. Historicals which \
                       are not involved in the cloning process will still be queried. `includeClones` means that broker \
                       queries any Historical without regarding clone status. This parameter only affects native queries. \
                       MSQ does not query Historicals directly.\
                       """
                   )
                   .language(Language.NATIVE)
                   .engine(Engine.NATIVE)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> OPTIMIZE_AGGREGATORS =
      booleanParameter("optimizeAggregators")
          .defaultValue(QueryContexts.DEFAULT_OPTIMIZE_AGGREGATORS)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_LEFT_SCAN_DIRECT =
     booleanParameter("enableJoinLeftTableScanDirect")
         .defaultValue(QueryContexts.DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT)
          .docs(
              doc().description(
                       """
                       This parameter applies to queries with joins. By default, when the left child is a simple scan \
                       with a filter, Druid runs the scan as a query, then joins it with the right child on the Broker. \
                       Setting this parameter to `true` overrides that behavior and pushes the join to the data servers \
                       instead. Even if a query doesn't explicitly include a join, this parameter may still apply since \
                       the SQL planner can translate the query into a join internally.\
                       """
                   )
                   .defaultDescription("`false`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> USE_FILTER_CNF = booleanParameter("useFilterCNF")
     .defaultValue(QueryContexts.DEFAULT_USE_FILTER_CNF)
      .docs(
          doc().description(
                   """
                   If true, Druid will attempt to convert the query filter to Conjunctive Normal Form (CNF). During \
                   query processing, columns can be pre-filtered by intersecting the bitmap indexes of all values that \
                   match the eligible filters, often greatly reducing the raw number of rows which need to be scanned. \
                   But this effect only happens for the top level filter, or individual clauses of a top level 'and' \
                   filter. As such, filters in CNF potentially have a higher chance to utilize a large amount of bitmap \
                   indexes on string columns during pre-filtering. However, this setting should be used with great \
                   caution, as it can sometimes have a negative effect on performance, and in some cases, the act of \
                   computing CNF of a filter can be expensive. We recommend hand tuning your filters to produce an \
                   optimal form if possible, or at least verifying through experimentation that using this parameter \
                   actually improves your query performance with no ill-effects.\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
     .build();

  public static final QueryContextParameter<Integer> NUM_RETRIES_ON_MISSING_SEGMENTS =
      integerParameter("numRetriesOnMissingSegments").build();

  public static final QueryContextParameter<Boolean> RETURN_PARTIAL_RESULTS =
      booleanParameter("returnPartialResults").build();

  public static final QueryContextParameter<Boolean> USE_CACHE = booleanParameter("useCache")
     .defaultValue(QueryContexts.DEFAULT_USE_CACHE)
      .docs(
          doc().description(
                   """
                   Flag indicating whether to leverage the query cache for this query. When set to false, it disables \
                   reading from the query cache for this query. When set to true, Apache Druid uses \
                   `druid.broker.cache.useCache` or `druid.historical.cache.useCache` to determine whether or not to read \
                   from the query cache.\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
     .build();

  public static final QueryContextParameter<Boolean> SECONDARY_PARTITION_PRUNING =
     booleanParameter("secondaryPartitionPruning")
         .defaultValue(QueryContexts.DEFAULT_SECONDARY_PARTITION_PRUNING)
          .docs(
              doc().description("Enable secondary partition pruning on the Broker. The Broker will always prune unnecessary segments from the input scan based on a filter on time intervals, but if the data is further partitioned with hash or range partitioning, this option will enable additional pruning based on a filter on secondary partition dimensions.")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> DEBUG = booleanParameter("debug")
     .defaultValue(QueryContexts.DEFAULT_ENABLE_DEBUG)
      .docs(
          doc().description("Flag indicating whether to enable debugging outputs for the query. When set to false, no additional logs will be produced (logs produced will be entirely dependent on your logging level). When set to true, the following addition logs will be produced:<br />- Log the stack trace of the exception (if any) produced by the query")
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
     .build();

  public static final QueryContextParameter<Boolean> BY_SEGMENT = booleanParameter("bySegment")
     .defaultValue(QueryContexts.DEFAULT_BY_SEGMENT)
      .docs(
          doc().description("Native queries only. Return \"by segment\" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from.")
               .language(Language.NATIVE)
               .engine(Engine.NATIVE)
               .build()
      )
     .build();

  public static final QueryContextParameter<String> BROKER_SERVICE =
      stringParameter("brokerService")
          .docs(
              doc().description("Broker service to which this query should be routed. This parameter is honored only by a broker selector strategy of type *manual*. See [Router strategies](../design/router.md#router-strategies) for more details.")
                   .defaultDescription("`null`")
                   .language(Language.NATIVE)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();


  public static final QueryContextParameter<Integer> IN_SUBQUERY_THRESHOLD =
     integerParameter("inSubQueryThreshold")
         .defaultValue(QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD)
          .docs(
              doc().description(
                       """
                       At or beyond this threshold number of values, Druid converts SQL `IN` to `JOIN` on an inline \
                       table. `inFunctionThreshold` takes priority over this setting. A threshold of 0 forces usage of \
                       an inline table in all cases where the size of a SQL `IN` is larger than `inFunctionThreshold`. \
                       A threshold of `2147483647` disables the rewrite of SQL `IN` to `JOIN`.\
                       """
                   )
                   .defaultDescription("`2147483647`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_THRESHOLD =
     integerParameter("inFunctionThreshold")
         .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_THRESHOLD)
          .docs(
              doc().description(
                       """
                       At or beyond this threshold number of values, Druid converts SQL `IN` to [`SCALAR_IN_ARRAY`](sql-\
                       functions.md#scalar_in_array). A threshold of 0 forces this conversion in all cases. A threshold \
                       of `Integer.MAX_VALUE` disables this conversion. The converted function is eligible for fewer \
                       planning-time optimizations, which speeds up planning, but may prevent certain planning-time \
                       optimizations.\
                       """
                   )
                   .defaultDescription("`100`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_EXPR_THRESHOLD =
     integerParameter("inFunctionExprThreshold")
         .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_EXPR_THRESHOLD)
          .docs(
              doc().description(
                       """
                       At or beyond this threshold number of values, SQL `IN` is eligible for execution using the native \
                       function `scalar_in_array` rather than an <code>&#124;&#124;</code> of `==`, even if the number of \
                       values is below `inFunctionThreshold`. This property only affects translation of SQL `IN` to a \
                       [native expression](math-expr.md). It doesn't affect translation of SQL `IN` to a [native filter](filters.md). \
                       This property is provided for backwards compatibility purposes, and may be removed in a future release.\
                       """
                   )
                   .defaultDescription("`2`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> ENABLE_TIME_BOUNDARY_PLANNING =
     booleanParameter("enableTimeBoundaryPlanning")
         .defaultValue(QueryContexts.DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING)
          .docs(
              doc().description(
                       """
                       If `true`, Druid converts SQL queries to [time boundary queries](timeboundaryquery.md) wherever \
                       possible. Time boundary queries are very efficient for min-max calculation on the `__time` column \
                       in a datasource.\
                       """
                   )
                   .defaultDescription("`false`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> POPULATE_CACHE =
     booleanParameter("populateCache")
         .defaultValue(QueryContexts.DEFAULT_POPULATE_CACHE)
          .docs(
              doc().description(
                       """
                       Flag indicating whether to save the results of the query to the query cache. Primarily used for \
                       debugging. When set to false, it disables saving the results of this query to the query cache. When \
                       set to true, Druid uses `druid.broker.cache.populateCache` or `druid.historical.cache.populateCache` \
                       to determine whether or not to save the results of this query to the query cache.\
                       """
                   )
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
         .build();

  public static final QueryContextParameter<Boolean> POPULATE_RESULT_LEVEL_CACHE =
     booleanParameter("populateResultLevelCache")
         .defaultValue(QueryContexts.DEFAULT_POPULATE_RESULTLEVEL_CACHE)
          .docs(
              doc().description(
                       """
                       Flag indicating whether to save the results of the query to the result level cache. Primarily used \
                       for debugging. When set to false, it disables saving the results of this query to the query cache. \
                       When set to true, Druid uses `druid.broker.cache.populateResultLevelCache` to determine whether or \
                       not to save the results of this query to the result-level query cache.\
                       """
                   )
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
         .build();


  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG =
      booleanParameter("serializeDateTimeAsLong")
      .docs(
          doc().description("If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process")
                   .defaultDescription("`false`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG_INNER =
      booleanParameter("serializeDateTimeAsLongInner")
      .docs(
          doc().description("If true, DateTime is serialized as long in the data transportation between Broker and compute process")
                   .defaultDescription("`false`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Integer> UNCOVERED_INTERVALS_LIMIT =
      integerParameter("uncoveredIntervalsLimit")
          .defaultValue(QueryContexts.DEFAULT_UNCOVERED_INTERVALS_LIMIT)
          .build();

  public static final QueryContextParameter<Integer> MIN_TOP_N_THRESHOLD =
      integerParameter("minTopNThreshold")
          .docs(
              doc().description("The top minTopNThreshold local results from each segment are returned for merging to determine the global topN.")
                   .defaultDescription("`1000`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .query(QueryType.TOP_N)
                   .build()
          )
          .build();

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
      stringParameter("sqlQueryId")
          .docs(
              doc().description(
                       """
                       SQL query ID. For HTTP client, Druid returns it in the `X-Druid-SQL-Query-Id` header.<br/><br/>To specify a \
                       SQL query ID, use `sqlQueryId` instead of [`queryId`](query-context-reference.md). Setting `queryId` \
                       for a SQL request has no effect. All native queries underlying SQL use an auto-generated `queryId`.\
                       """
                   )
                   .defaultDescription("auto-generated")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
          .build();

  public static final QueryContextParameter<Boolean> SQL_STRINGIFY_ARRAYS =
      booleanParameter("sqlStringifyArrays")
          .docs(
              doc().description("If `true`, Druid serializes result columns with array values as JSON strings in the response instead of arrays.")
                   .defaultDescription("`true`, except for JDBC connections, where it's always `false`")
                   .language(Language.SQL)
                   .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
                   .build()
          )
          .build();

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
          .docs(
              doc().description(
                       """
                       **Deprecated.** Use `realtimeSegmentsMode=exclusive` instead. When set to `true`, this is \
                       equivalent to `realtimeSegmentsMode=exclusive`. When set to `false`, this is equivalent to \
                       `realtimeSegmentsMode=include`.\
                       """
                   )
                   .defaultDescription("`false`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
         .build();

  public static final QueryContextParameter<QueryContexts.RealtimeSegmentsMode> REALTIME_SEGMENTS_MODE =
     enumParameter("realtimeSegmentsMode", QueryContexts.RealtimeSegmentsMode.class)
         .defaultValue(QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE)
          .docs(
              doc().description("Controls whether realtime segments are queried. `include` queries all segments, including realtime. `exclude` skips realtime segments. `exclusive` queries only realtime segments.")
                   .defaultDescription("`include`")
                   .language(Language.NATIVE, Language.SQL)
                   .engine(Engine.NATIVE)
                   .build()
          )
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
                   .defaultDescription("`druid.query.scan.maxRowsQueuedForOrdering`")
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
