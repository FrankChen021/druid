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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.BatchedInlineDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.handler.SystemTableNodeLocator;
import org.apache.druid.server.system.handler.SystemTableQueryClient;
import org.apache.druid.server.system.handler.SystemTableQueryHandler;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Compares Bindable and native execution of the Web Console datasource-tab query over 500,000 segments. */
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsAppend = {
        "-Xmx12g",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
    }
)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SysSegmentsSqlBenchmark
{
  private static final int NUM_SEGMENTS = 500_000;
  private static final int NUM_DATASOURCES = 1_000;
  private static final String BINDABLE = "bindable";
  private static final String NATIVE_ROW = "nativeRow";
  private static final String NATIVE_PROVIDER = "nativeProvider";
  private static final String SQL = "SELECT\n"
                                    + "datasource,\n"
                                    + "COUNT(*) FILTER (WHERE is_active = 1) AS num_segments,\n"
                                    + "COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 "
                                    + "AND replication_factor = 0) AS num_zero_replica_segments,\n"
                                    + "COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 "
                                    + "AND is_available = 0 AND replication_factor > 0) AS num_segments_to_load,\n"
                                    + "COUNT(*) FILTER (WHERE is_available = 1 AND is_active = 0) "
                                    + "AS num_segments_to_drop,\n"
                                    + "SUM(\"size\") FILTER (WHERE is_active = 1) AS total_data_size,\n"
                                    + "MIN(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) "
                                    + "AS min_segment_rows,\n"
                                    + "AVG(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) "
                                    + "AS avg_segment_rows,\n"
                                    + "MAX(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) "
                                    + "AS max_segment_rows,\n"
                                    + "SUM(\"num_rows\") FILTER (WHERE is_active = 1) AS total_rows,\n"
                                    + "CASE WHEN SUM(\"num_rows\") FILTER (WHERE is_available = 1) <> 0 "
                                    + "THEN (SUM(\"size\") FILTER (WHERE is_available = 1) / "
                                    + "SUM(\"num_rows\") FILTER (WHERE is_available = 1)) ELSE 0 END "
                                    + "AS avg_row_size,\n"
                                    + "SUM(\"size\" * \"num_replicas\") FILTER (WHERE is_active = 1) "
                                    + "AS replicated_size\n"
                                    + "FROM sys.segments\n"
                                    + "GROUP BY 1\n"
                                    + "ORDER BY 1";

  private static final Map<String, Object> BINDABLE_CONTEXT = ImmutableMap.of(
      PlannerContext.CTX_USE_NATIVE_QUERY_FOR_SYSTEM_TABLES,
      false
  );
  private static final Map<String, Object> NATIVE_CONTEXT = ImmutableMap.of(
      PlannerContext.CTX_USE_NATIVE_QUERY_FOR_SYSTEM_TABLES,
      true
  );

  private final Closer closer = Closer.create();
  private PlannerFactory plannerFactory;
  private SqlEngine rowEngine;
  private SqlEngine providerEngine;

  @State(Scope.Thread)
  public static class ExecutionState
  {
    @Param({BINDABLE, NATIVE_ROW, NATIVE_PROVIDER})
    private String executionPath;
    private PreparedQuery preparedQuery;

    @Setup(Level.Invocation)
    public void setup(final SysSegmentsSqlBenchmark benchmark)
    {
      preparedQuery = benchmark.prepare(executionPath);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
    {
      preparedQuery.close();
    }
  }

  private static class PreparedQuery implements AutoCloseable
  {
    private final DruidPlanner planner;
    private final PlannerResult plannerResult;

    PreparedQuery(final DruidPlanner planner, final PlannerResult plannerResult)
    {
      this.planner = planner;
      this.plannerResult = plannerResult;
    }

    @Override
    public void close()
    {
      planner.close();
    }
  }

  private static class EmptyBrokerSegmentMetadataCache extends BrokerSegmentMetadataCache
  {
    EmptyBrokerSegmentMetadataCache()
    {
      super(
          EasyMock.mock(QueryLifecycleFactory.class),
          EasyMock.mock(TimelineServerView.class),
          BrokerSegmentMetadataCacheConfig.create(),
          EasyMock.mock(org.apache.druid.server.security.Escalator.class),
          EasyMock.mock(InternalQueryConfig.class),
          new NoopServiceEmitter(),
          new PhysicalDatasourceMetadataFactory(
              EasyMock.mock(JoinableFactory.class),
              EasyMock.mock(SegmentManager.class)
          ),
          new NoopCoordinatorClient(),
          CentralizedDatasourceSchemaConfig.create()
      );
    }
  }

  /** Keeps the former row-only native path available as a stable benchmark baseline. */
  private static class RowOnlySystemTableDataProvider implements SystemTableDataProvider
  {
    private final SystemTableDataProvider delegate;

    RowOnlySystemTableDataProvider(final SystemTableDataProvider delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public List<SystemTablePushdownFilter> getPushdownFilters()
    {
      return delegate.getPushdownFilters();
    }

    @Override
    public Iterable<Object[]> getRows(
        final List<DimFilter> filters,
        final AuthenticationResult internalAuthenticationResult
    )
    {
      return delegate.getRows(filters, internalAuthenticationResult);
    }

    @Override
    public Iterable<Object[]> getRawRows(
        final List<DimFilter> filters,
        final AuthenticationResult internalAuthenticationResult
    )
    {
      return delegate.getRawRows(filters, internalAuthenticationResult);
    }

    @Override
    public Object[] projectRow(final Object[] row, final int[] projects)
    {
      return delegate.projectRow(row, projects);
    }
  }

  @Setup(Level.Trial)
  public void setup()
  {
    final List<SegmentStatusInCluster> segments = buildSegments();
    final CoordinatorClient coordinatorClient = new NoopCoordinatorClient()
    {
      @Override
      public ListenableFuture<CloseableIterator<SegmentStatusInCluster>> fetchAllUsedSegmentsWithOvershadowedStatus(
          final Set<String> watchedDataSources,
          final boolean includeRealtimeSegments
      )
      {
        return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(segments.iterator()));
      }
    };
    final BrokerSegmentMetadataCacheConfig metadataConfig = CalciteTests.getJsonMapper().convertValue(
        ImmutableMap.of("metadataSegmentCacheEnable", false),
        BrokerSegmentMetadataCacheConfig.class
    );
    final MetadataSegmentView metadataView = new MetadataSegmentView(
        coordinatorClient,
        new BrokerSegmentWatcherConfig(),
        metadataConfig,
        NoopServiceEmitter.instance()
    );
    final BrokerSegmentMetadataCache segmentMetadataCache = new EmptyBrokerSegmentMetadataCache();
    final SegmentsTableDescriptor descriptor = new SegmentsTableDescriptor();
    final SegmentsTableDataProvider dataProvider = new SegmentsTableDataProvider(
        () -> segmentMetadataCache,
        metadataView,
        CalciteTests.getJsonMapper()
    );

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    final SpecificSegmentsQuerySegmentWalker walker = closer.register(
        SpecificSegmentsQuerySegmentWalker.createWalker(
            QueryStackTests.injectorWithLookup(),
            conglomerate,
            new MapSegmentWrangler(
                Map.of(
                    InlineDataSource.class,
                    new InlineSegmentWrangler(),
                    BatchedInlineDataSource.class,
                    new BatchedInlineDataSource.Wrangler()
                )
            ),
            new JoinableFactoryWrapper(QueryStackTests.makeJoinableFactoryFromDefault(null, null, null)),
            QueryStackTests.DEFAULT_NOOP_SCHEDULER
        )
    );
    final SystemTableQueryHandler providerQueryHandler = new SystemTableQueryHandler(
        Map.<String, SystemTableDataProvider>of(descriptor.getTableName(), dataProvider),
        Map.<String, SystemTableDescriptor>of(descriptor.getTableName(), descriptor),
        new ScanQueryEngine(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
    final SystemTableQueryHandler rowQueryHandler = new SystemTableQueryHandler(
        Map.<String, SystemTableDataProvider>of(
            descriptor.getTableName(),
            new RowOnlySystemTableDataProvider(dataProvider)
        ),
        Map.<String, SystemTableDescriptor>of(descriptor.getTableName(), descriptor),
        new ScanQueryEngine(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
    rowEngine = makeEngine(conglomerate, walker, descriptor, rowQueryHandler);
    providerEngine = makeEngine(conglomerate, walker, descriptor, providerQueryHandler);

    final PlannerConfig plannerConfig = new PlannerConfig();
    final TimelineServerView timelineServerView = new TestTimelineServerView(Collections.emptyList());
    final DruidSchemaProvider druidSchemaProvider = QueryFrameworkUtils.createMockSchemaProvider(
        CalciteTests.INJECTOR,
        conglomerate,
        walker,
        new NoopDruidSchemaManager(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CatalogResolver.NULL_RESOLVER,
        timelineServerView
    );
    final SystemSchemaProvider systemSchemaProvider = new SystemSchemaProvider(
        segmentMetadataCache,
        metadataView,
        timelineServerView,
        EasyMock.mock(org.apache.druid.client.FilteredServerInventoryView.class),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinatorClient,
        new NoopOverlordClient(),
        CalciteTests.mockDruidNodeDiscoveryProvider(CalciteTests.mockCoordinatorNode()),
        CalciteTests.getJsonMapper(),
        EasyMock.mock(org.apache.druid.java.util.http.client.HttpClient.class),
        () -> new org.apache.druid.sql.http.SqlEngineRegistry(Collections.emptySet()),
        plannerConfig
    );
    final DruidSchemaCatalogProvider schemaProvider = QueryFrameworkUtils.createMockRootSchemaProvider(
        new NoopViewManager(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        druidSchemaProvider,
        systemSchemaProvider,
        QueryFrameworkUtils.createMockLookupSchema(CalciteTests.INJECTOR),
        QueryFrameworkUtils.createOperatorTable(CalciteTests.INJECTOR),
        plannerConfig
    );

    plannerFactory = new PlannerFactory(
        schemaProvider,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new DruidHookDispatcher()
    );

    final List<Object[]> bindableResults = runQuery(BINDABLE);
    for (final String executionPath : List.of(NATIVE_ROW, NATIVE_PROVIDER)) {
      final List<Object[]> nativeResults = runQuery(executionPath);
      if (bindableResults.size() != NUM_DATASOURCES || !rowsEqual(bindableResults, nativeResults)) {
        throw new IllegalStateException("Bindable and native benchmark results do not match for " + executionPath);
      }
    }
  }

  private static SqlEngine makeEngine(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final SystemTableDescriptor descriptor,
      final SystemTableQueryHandler localQueryHandler
  )
  {
    final SystemTableQueryClient queryClient = new SystemTableQueryClient(
        EasyMock.mock(SystemTableNodeLocator.class),
        EasyMock.mock(DirectDruidClientFactory.class),
        EasyMock.mock(QueryScheduler.class),
        walker,
        Map.of(descriptor.getTableName(), descriptor),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        localQueryHandler,
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        CalciteTests.mockCoordinatorNode()
    );
    final QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        conglomerate,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new DefaultQueryConfig(Map.of()),
        Map.<Class<? extends DataSource>, org.apache.druid.server.DataSourceQueryHandler>of(
            SystemTableDataSource.class,
            queryClient
        ),
        null
    );
    return new NativeSqlEngine(queryLifecycleFactory, CalciteTests.getJsonMapper(), (SqlStatementFactory) null);
  }

  private static List<SegmentStatusInCluster> buildSegments()
  {
    final List<SegmentStatusInCluster> segments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      final String dataSource = StringUtils.format("datasource_%d", i % NUM_DATASOURCES);
      final int dayOffset = i / NUM_DATASOURCES;
      final SegmentId segmentId = SegmentId.of(
          dataSource,
          Intervals.utc(dayOffset * 86_400_000L, (dayOffset + 1) * 86_400_000L),
          "1",
          new LinearShardSpec(0)
      );
      final DataSegment segment = DataSegment.builder(segmentId).size(1_000L).totalRows(100).build();
      segments.add(new SegmentStatusInCluster(segment, false, 1, 100L, false));
    }
    segments.sort(Comparator.naturalOrder());
    return ImmutableList.copyOf(segments);
  }

  private static boolean rowsEqual(final List<Object[]> left, final List<Object[]> right)
  {
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (!java.util.Arrays.deepEquals(left.get(i), right.get(i))) {
        return false;
      }
    }
    return true;
  }

  private PreparedQuery prepare(final String executionPath)
  {
    final SqlEngine engine;
    final Map<String, Object> context;
    switch (executionPath) {
      case BINDABLE:
        engine = rowEngine;
        context = BINDABLE_CONTEXT;
        break;
      case NATIVE_ROW:
        engine = rowEngine;
        context = NATIVE_CONTEXT;
        break;
      case NATIVE_PROVIDER:
        engine = providerEngine;
        context = NATIVE_CONTEXT;
        break;
      default:
        throw new IllegalArgumentException("Unknown execution path " + executionPath);
    }

    final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, SQL, context);
    try {
      return new PreparedQuery(planner, planner.plan());
    }
    catch (RuntimeException | Error t) {
      planner.close();
      throw t;
    }
  }

  private List<Object[]> runQuery(final String executionPath)
  {
    try (final PreparedQuery preparedQuery = prepare(executionPath)) {
      return preparedQuery.plannerResult.run().getResults().toList();
    }
  }

  @Benchmark
  public void queryBindable(final Blackhole blackhole)
  {
    blackhole.consume(runQuery(BINDABLE));
  }

  @Benchmark
  public void queryNative(final Blackhole blackhole)
  {
    blackhole.consume(runQuery(NATIVE_ROW));
  }

  @Benchmark
  public void queryNativeProvider(final Blackhole blackhole)
  {
    blackhole.consume(runQuery(NATIVE_PROVIDER));
  }

  @Benchmark
  public void queryExecutionOnly(final ExecutionState state, final Blackhole blackhole)
  {
    final Sequence<Object[]> resultSequence = state.preparedQuery.plannerResult.run().getResults();
    blackhole.consume(resultSequence.toList());
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }
}
