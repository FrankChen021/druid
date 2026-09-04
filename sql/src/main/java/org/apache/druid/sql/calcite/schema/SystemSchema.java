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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.inject.Provider;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.server.system.table.SegmentsTableDescriptor;
import org.apache.druid.server.system.table.ServerSegmentsTableDescriptor;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.QueryInfo;
import org.apache.druid.sql.http.SqlEngineRegistry;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SystemSchema extends AbstractTableSchema
{
  public static final String SEGMENTS_TABLE = SegmentsTableDescriptor.TABLE_NAME;
  public static final String SERVERS_TABLE = "servers";
  public static final String SERVER_SEGMENTS_TABLE = ServerSegmentsTableDescriptor.TABLE_NAME;
  public static final String SUPERVISOR_TABLE = "supervisors";
  public static final String QUERIES_TABLE = "queries";

  private static final Function<DataSegment, Iterable<ResourceAction>> SEGMENT_RA_GENERATOR =
      segment -> Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
          segment.getDataSource())
      );

  public static boolean canUseNativeSystemTable(
      final RelOptTable table,
      final PlannerContext plannerContext
  )
  {
    return plannerContext.useNativeQueryForSystemTables()
           && NativeSqlEngine.NAME.equals(plannerContext.getEngine().name())
           && table.unwrap(NativeSystemTable.class) != null;
  }

  /**
   * Returns the native representation advertised by a system table resolved through {@link SystemSchemaProvider}.
   * Eligibility for native planning must be checked with {@link #canUseNativeSystemTable} before calling this method.
   */
  @Nullable
  public static DruidTable getNativeSystemTable(final RelOptTable table)
  {
    final NativeSystemTable nativeSystemTable = table.unwrap(NativeSystemTable.class);
    return nativeSystemTable == null ? null : nativeSystemTable.asNativeTable();
  }

  static final RowSignature SEGMENTS_SIGNATURE = SegmentsTableDescriptor.ROW_SIGNATURE;

  static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("server_type", ColumnType.STRING)
      .add("tier", ColumnType.STRING)
      .add("curr_size", ColumnType.LONG)
      .add("max_size", ColumnType.LONG)
      .add("storage_size", ColumnType.LONG)
      .add("is_leader", ColumnType.LONG)
      .add("start_time", ColumnType.STRING)
      .add("version", ColumnType.STRING)
      .add("build_revision", ColumnType.STRING)
      .add("labels", ColumnType.STRING)
      .add("available_processors", ColumnType.LONG)
      .add("total_memory", ColumnType.LONG)
      .build();

  static final RowSignature SUPERVISOR_SIGNATURE = RowSignature
      .builder()
      .add("supervisor_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("detailed_state", ColumnType.STRING)
      .add("healthy", ColumnType.LONG)
      .add("type", ColumnType.STRING)
      .add("source", ColumnType.STRING)
      .add("suspended", ColumnType.LONG)
      .add("spec", ColumnType.STRING)
      .build();

  static final RowSignature QUERIES_SIGNATURE = RowSignature
      .builder()
      .add("id", ColumnType.STRING)
      .add("engine", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("info", ColumnType.STRING)
      .build();

  /**
   * Index of the "info" column in {@link #QUERIES_SIGNATURE}. Used for projection pushdown.
   */
  private static final int QUERIES_INFO_INDEX = QUERIES_SIGNATURE.indexOf("info");

  /**
   * List of [0..n) where n is the size of {@link #QUERIES_SIGNATURE}.
   */
  private static final int[] QUERIES_PROJECT_ALL = IntStream.range(0, QUERIES_SIGNATURE.size()).toArray();

  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final MetadataSegmentView metadataView;
  private final TimelineServerView serverView;
  private final FilteredServerInventoryView serverInventoryView;
  private final AuthorizerMapper authorizerMapper;
  private final CoordinatorClient coordinatorClient;
  private final OverlordClient overlordClient;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final Provider<SqlEngineRegistry> sqlEngineRegistryProvider;
  private final PlannerConfig plannerConfig;
  private final AuthenticationResult authenticationResult;
  private final Set<String> allTableNames;

  public SystemSchema(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final MetadataSegmentView metadataView,
      final TimelineServerView serverView,
      final FilteredServerInventoryView serverInventoryView,
      final AuthorizerMapper authorizerMapper,
      final CoordinatorClient coordinatorClient,
      final OverlordClient overlordClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ObjectMapper jsonMapper,
      final HttpClient httpClient,
      final Provider<SqlEngineRegistry> sqlEngineRegistryProvider,
      final PlannerConfig plannerConfig,
      final AuthenticationResult authenticationResult,
      final Set<String> allTableNames
  )
  {
    this.segmentMetadataCache = segmentMetadataCache;
    this.metadataView = metadataView;
    this.serverView = serverView;
    this.serverInventoryView = serverInventoryView;
    this.authorizerMapper = authorizerMapper;
    this.coordinatorClient = coordinatorClient;
    this.overlordClient = overlordClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.sqlEngineRegistryProvider = sqlEngineRegistryProvider;
    this.plannerConfig = plannerConfig;
    this.authenticationResult = authenticationResult;
    this.allTableNames = allTableNames;
  }

  @Override
  @Nullable
  public Table getTable(String name)
  {
    if (!isTableVisible(name)) {
      return null;
    }

    return switch (name) {
      case SEGMENTS_TABLE -> new SegmentsTable(
          segmentMetadataCache,
          metadataView,
          jsonMapper,
          authorizerMapper,
          authenticationResult
      );
      case SERVERS_TABLE -> new ServersTable(
          druidNodeDiscoveryProvider,
          serverInventoryView,
          authorizerMapper,
          overlordClient,
          coordinatorClient,
          jsonMapper,
          authenticationResult
      );
      case SERVER_SEGMENTS_TABLE -> new ServerSegmentsTable(serverView, authorizerMapper, authenticationResult);
      case TaskTableDescriptor.TABLE_NAME -> new TasksTable(overlordClient, authorizerMapper, authenticationResult);
      case SUPERVISOR_TABLE -> new SupervisorsTable(overlordClient, authorizerMapper, authenticationResult);
      case SystemServerPropertiesTable.TABLE_NAME -> new SystemServerPropertiesTable(
          druidNodeDiscoveryProvider,
          authorizerMapper,
          httpClient,
          jsonMapper,
          authenticationResult
      );
      case QUERIES_TABLE -> new QueriesTable(
          sqlEngineRegistryProvider,
          jsonMapper,
          authorizerMapper,
          authenticationResult
      );
      case null, default -> throw DruidException.defensive("Unrecognized table name[%s]", name);
    };
  }

  @Override
  public Set<String> getTableNames()
  {
    if (plannerConfig.isAuthorizeTableVisibility()) {
      return SchemaUtils.filterVisibleTables(
          authorizerMapper,
          authenticationResult,
          allTableNames,
          _ -> plannerConfig.isAuthorizeSystemTablesDirectly() ? ResourceType.SYSTEM_TABLE : null
      );
    } else {
      // sys table authorization is not enabled, so all sys tables are visible to all users.
      return allTableNames;
    }
  }

  /**
   * Returns whether a sys table with a particular name should be visible to the provided user.
   */
  private boolean isTableVisible(final String sysTableName)
  {
    if (!allTableNames.contains(sysTableName)) {
      // Short circuit that hides sys.queries if it is disabled server-wide.
      return false;
    } else if (plannerConfig.isAuthorizeTableVisibility()) {
      return SchemaUtils.isTableVisible(
          authorizerMapper,
          authenticationResult,
          sysTableName,
          _ -> plannerConfig.isAuthorizeSystemTablesDirectly() ? ResourceType.SYSTEM_TABLE : null
      );
    } else {
      // sys table authorization is not enabled, so all sys tables are visible to all users.
      return true;
    }
  }

  /**
   * This table contains row per segment from metadata store as well as served segments.
   */
  static class SegmentsTable extends AbstractTable implements ProjectableFilterableTable, NativeSystemTable
  {
    private static final int DATASOURCE_COLUMN = SEGMENTS_SIGNATURE.indexOf("datasource");

    private final BrokerSegmentMetadataCache segmentMetadataCache;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final MetadataSegmentView metadataView;
    private final AuthenticationResult authenticationResult;

    public SegmentsTable(
        BrokerSegmentMetadataCache segmentMetadataCache,
        MetadataSegmentView metadataView,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper,
        AuthenticationResult authenticationResult
    )
    {
      this.segmentMetadataCache = segmentMetadataCache;
      this.metadataView = metadataView;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
      this.authenticationResult = authenticationResult;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SEGMENTS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public DruidTable asNativeTable()
    {
      return new NativeSegmentsTable();
    }

    @Override
    public Enumerable<Object[]> scan(
        final DataContext root,
        final List<RexNode> filters,
        @Nullable final int[] projects
    )
    {
      final Set<String> dataSourceFilter = getDataSourceFilter(filters);
      final Iterable<Object[]> authorizedRows = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          Iterables.filter(
              SegmentsTableDataProvider.getRawRows(segmentMetadataCache, metadataView, dataSourceFilter),
              Objects::nonNull
          ),
          row -> Collections.singletonList(
              AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply((String) row[DATASOURCE_COLUMN])
          ),
          authorizerMapper
      );
      return Linq4j.asEnumerable(authorizedRows)
                   .where(Objects::nonNull)
                   .select(row -> SegmentsTableDataProvider.projectRow(row, projects, jsonMapper));
    }

    /**
     * Best-effort extraction of an exact-match {@code datasource} constraint (column
     * {@link #DATASOURCE_COLUMN}) from the pushed-down filters, so sys.segments can restrict its scan
     * to the matching datasources rather than materializing every segment in the cluster. Delegates to
     * {@link SystemSchemaFilters}, which handles {@code datasource = 'x'}, {@code datasource IN (...)},
     * OR-of-equalities, and nested {@code AND}/{@code OR}. Returns {@code null} when no usable
     * datasource predicate is present, in which case the previous full-scan behavior is retained.
     */
    @Nullable
    static Set<String> getDataSourceFilter(List<RexNode> filters)
    {
      return SystemSchemaFilters.extractColumnValues(filters, DATASOURCE_COLUMN);
    }

  }

  /**
   * This table contains row per server. It contains all the discovered servers in Druid cluster.
   * Some columns like tier and size are only applicable to historical nodes which contain segments.
   */
  static class ServersTable extends AbstractTable implements ScannableTable
  {
    // This is used for maxSize and currentSize when they are unknown.
    // The unknown size doesn't have to be 0, it's better to be null.
    // However, this table is returning 0 for them for some reason and we keep the behavior for backwards compatibility.
    // Maybe we can remove this and return nulls instead when we remove the bindable query path which is currently
    // used to query system tables.
    private static final long UNKNOWN_SIZE = 0L;

    private final AuthorizerMapper authorizerMapper;
    private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
    private final FilteredServerInventoryView serverInventoryView;
    private final OverlordClient overlordClient;
    private final CoordinatorClient coordinatorClient;
    private final ObjectMapper jsonMapper;
    private final AuthenticationResult authenticationResult;

    public ServersTable(
        DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        FilteredServerInventoryView serverInventoryView,
        AuthorizerMapper authorizerMapper,
        OverlordClient overlordClient,
        CoordinatorClient coordinatorClient,
        ObjectMapper jsonMapper,
        AuthenticationResult authenticationResult
    )
    {
      this.authorizerMapper = authorizerMapper;
      this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
      this.serverInventoryView = serverInventoryView;
      this.overlordClient = overlordClient;
      this.coordinatorClient = coordinatorClient;
      this.jsonMapper = jsonMapper;
      this.authenticationResult = authenticationResult;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SERVERS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final Iterator<DiscoveryDruidNode> druidServers = getDruidServers(druidNodeDiscoveryProvider);
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      String tmpCoordinatorLeader = "";
      String tmpOverlordLeader = "";

      try {
        tmpCoordinatorLeader = FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true).toString();
      }
      catch (Exception ignored) {
        // no reason to kill the results if something is sad and there are no leaders
      }

      try {
        tmpOverlordLeader = FutureUtils.getUnchecked(overlordClient.findCurrentLeader(), true).toString();
      }
      catch (Exception ignored) {
        // no reason to kill the results if something is sad and there are no leaders
      }

      final String coordinatorLeader = tmpCoordinatorLeader;
      final String overlordLeader = tmpOverlordLeader;

      final FluentIterable<Object[]> results = FluentIterable
          .from(() -> druidServers)
          .transform((DiscoveryDruidNode discoveryDruidNode) -> {
            //noinspection ConstantConditions
            final boolean isDiscoverableDataServer = isDiscoverableDataServer(
                discoveryDruidNode.getService(DataNodeService.DISCOVERY_SERVICE_KEY, DataNodeService.class)
            );
            final NodeRole serverRole = discoveryDruidNode.getNodeRole();

            if (isDiscoverableDataServer) {
              final DruidServer druidServer = serverInventoryView.getInventoryValue(
                  discoveryDruidNode.getDruidNode().getHostAndPortToUse()
              );
              if (druidServer != null || NodeRole.HISTORICAL.equals(serverRole)) {
                // Build a row for the data server if that server is in the server view, or the node type is historical.
                // The historicals are usually supposed to be found in the server view. If some historicals are
                // missing, it could mean that there are some problems in them to announce themselves. We just fill
                // their status with nulls in this case.
                return buildRowForDiscoverableDataServer(discoveryDruidNode, druidServer);
              } else {
                return buildRowForNonDataServer(discoveryDruidNode);
              }
            } else if (NodeRole.COORDINATOR.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  coordinatorLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else if (NodeRole.OVERLORD.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  overlordLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else {
              return buildRowForNonDataServer(discoveryDruidNode);
            }
          });
      return Linq4j.asEnumerable(results);
    }


    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private Object[] buildRowForNonDataServer(DiscoveryDruidNode discoveryDruidNode)
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          null,
          toStringOrNull(discoveryDruidNode.getStartTime()),
          node.getVersion(),
          node.getBuildRevision(),
          node.getLabels() == null ? null : JacksonUtils.writeValueAsString(jsonMapper, node.getLabels()),
          (long) discoveryDruidNode.getAvailableProcessors(),
          discoveryDruidNode.getTotalMemory()
      };
    }

    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private Object[] buildRowForNonDataServerWithLeadership(
        DiscoveryDruidNode discoveryDruidNode,
        boolean isLeader
    )
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          isLeader ? 1L : 0L,
          toStringOrNull(discoveryDruidNode.getStartTime()),
          node.getVersion(),
          node.getBuildRevision(),
          node.getLabels() == null ? null : JacksonUtils.writeValueAsString(jsonMapper, node.getLabels()),
          (long) discoveryDruidNode.getAvailableProcessors(),
          discoveryDruidNode.getTotalMemory()
      };
    }

    /**
     * Returns a row for discoverable data server. This method prefers the information from
     * {@code serverFromInventoryView} if available which is the current state of the server. Otherwise, it
     * will get the information from {@code discoveryDruidNode} which has only static configurations.
     */
    private Object[] buildRowForDiscoverableDataServer(
        DiscoveryDruidNode discoveryDruidNode,
        @Nullable DruidServer serverFromInventoryView
    )
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      final DruidServer druidServerToUse = serverFromInventoryView == null
                                           ? toDruidServer(discoveryDruidNode)
                                           : serverFromInventoryView;
      final long currentSize;
      if (serverFromInventoryView == null) {
        // If server is missing in serverInventoryView, the currentSize should be unknown
        currentSize = UNKNOWN_SIZE;
      } else {
        currentSize = serverFromInventoryView.getCurrSize();
      }
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          druidServerToUse.getTier(),
          currentSize,
          druidServerToUse.getMaxSize(),
          druidServerToUse.getStorageSize(),
          null,
          toStringOrNull(discoveryDruidNode.getStartTime()),
          node.getVersion(),
          node.getBuildRevision(),
          node.getLabels() == null ? null : JacksonUtils.writeValueAsString(jsonMapper, node.getLabels()),
          (long) discoveryDruidNode.getAvailableProcessors(),
          discoveryDruidNode.getTotalMemory()
      };
    }

    private static boolean isDiscoverableDataServer(DataNodeService dataNodeService)
    {
      return dataNodeService != null && dataNodeService.isDiscoverable();
    }

    private static DruidServer toDruidServer(DiscoveryDruidNode discoveryDruidNode)
    {
      final DruidNode druidNode = discoveryDruidNode.getDruidNode();
      final DataNodeService dataNodeService = discoveryDruidNode.getService(
          DataNodeService.DISCOVERY_SERVICE_KEY,
          DataNodeService.class
      );
      if (isDiscoverableDataServer(dataNodeService)) {
        return new DruidServer(
            druidNode.getHostAndPortToUse(),
            druidNode.getHostAndPort(),
            druidNode.getHostAndTlsPort(),
            dataNodeService.getMaxSize(),
            dataNodeService.getStorageSize(),
            dataNodeService.getServerType(),
            dataNodeService.getTier(),
            dataNodeService.getPriority()
        );
      } else {
        throw new ISE("[%s] is not a discoverable data server", discoveryDruidNode);
      }
    }

  }

  /**
   * This table contains row per segment per server.
   */
  static class ServerSegmentsTable extends AbstractTable implements ScannableTable, NativeSystemTable
  {
    private final TimelineServerView serverView;
    private final AuthorizerMapper authorizerMapper;
    private final AuthenticationResult authenticationResult;

    public ServerSegmentsTable(
        TimelineServerView serverView,
        AuthorizerMapper authorizerMapper,
        AuthenticationResult authenticationResult
    )
    {
      this.serverView = serverView;
      this.authorizerMapper = authorizerMapper;
      this.authenticationResult = authenticationResult;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(ServerSegmentsTableDescriptor.ROW_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public DruidTable asNativeTable()
    {
      return new NativeServerSegmentsTable();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final int serverSegmentsTableSize = ServerSegmentsTableDescriptor.ROW_SIGNATURE.size();
      for (ImmutableDruidServer druidServer : druidServers) {
        final Iterable<DataSegment> authorizedServerSegments = AuthorizationUtils.filterAuthorizedResources(
            authenticationResult,
            druidServer.iterateAllSegments(),
            SEGMENT_RA_GENERATOR,
            authorizerMapper
        );

        for (DataSegment segment : authorizedServerSegments) {
          Object[] row = new Object[serverSegmentsTableSize];
          row[0] = druidServer.getHost();
          row[1] = segment.getId().toString();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * This table contains row per task.
   */
  static class TasksTable extends AbstractTable implements ScannableTable, NativeSystemTable
  {
    private final OverlordClient overlordClient;
    private final AuthorizerMapper authorizerMapper;
    private final AuthenticationResult authenticationResult;

    public TasksTable(
        OverlordClient overlordClient,
        AuthorizerMapper authorizerMapper,
        AuthenticationResult authenticationResult
    )
    {
      this.overlordClient = overlordClient;
      this.authorizerMapper = authorizerMapper;
      this.authenticationResult = authenticationResult;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(TaskTableDescriptor.ROW_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public DruidTable asNativeTable()
    {
      return new NativeTasksTable();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class TasksEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<TaskStatusPlus> it;

        public TasksEnumerable(CloseableIterator<TaskStatusPlus> tasks)
        {
          this.it = getAuthorizedTasks(tasks);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<>()
          {
            @Override
            public Object[] current()
            {
              final TaskStatusPlus task = it.next();

              return new Object[]{
                  task.getId(),
                  task.getGroupId(),
                  task.getType(),
                  task.getDataSource(),
                  toStringOrNull(task.getCreatedTime()),
                  toStringOrNull(task.getQueueInsertionTime()),
                  toStringOrNull(task.getStatusCode()),
                  toStringOrNull(task.getRunnerStatusCode()),
                  task.getDuration() == null ? 0L : task.getDuration(),
                  task.getLocation().getLocation(),
                  task.getLocation().getHost(),
                  (long) task.getLocation().getPort(),
                  (long) task.getLocation().getTlsPort(),
                  task.getErrorMsg()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new TasksEnumerable(FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, null), true));
    }

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(CloseableIterator<TaskStatusPlus> it)
    {
      Function<TaskStatusPlus, Iterable<ResourceAction>> raGenerator = task -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(task.getDataSource()));

      final Iterable<TaskStatusPlus> authorizedTasks = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedTasks.iterator(), it);
    }

  }

  /**
   * This table contains a row per supervisor task.
   */
  static class SupervisorsTable extends AbstractTable implements ScannableTable
  {
    private final OverlordClient overlordClient;
    private final AuthorizerMapper authorizerMapper;
    private final AuthenticationResult authenticationResult;

    public SupervisorsTable(
        OverlordClient overlordClient,
        AuthorizerMapper authorizerMapper,
        AuthenticationResult authenticationResult
    )
    {
      this.overlordClient = overlordClient;
      this.authorizerMapper = authorizerMapper;
      this.authenticationResult = authenticationResult;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SUPERVISOR_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class SupervisorsEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<SupervisorStatus> it;

        public SupervisorsEnumerable(CloseableIterator<SupervisorStatus> tasks)
        {
          this.it = getAuthorizedSupervisors(tasks);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<>()
          {
            @Override
            public Object[] current()
            {
              final SupervisorStatus supervisor = it.next();
              return new Object[]{
                  supervisor.getId(),
                  supervisor.getDataSource(),
                  supervisor.getState(),
                  supervisor.getDetailedState(),
                  supervisor.isHealthy() ? 1L : 0L,
                  supervisor.getType(),
                  supervisor.getSource(),
                  supervisor.isSuspended() ? 1L : 0L,
                  supervisor.getSpecString()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new SupervisorsEnumerable(FutureUtils.getUnchecked(overlordClient.supervisorStatuses(), true));
    }

    private CloseableIterator<SupervisorStatus> getAuthorizedSupervisors(CloseableIterator<SupervisorStatus> it)
    {
      Function<SupervisorStatus, Iterable<ResourceAction>> raGenerator = supervisor -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(supervisor.getDataSource()));

      final Iterable<SupervisorStatus> authorizedSupervisors = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedSupervisors.iterator(), it);
    }
  }

  private static <T> CloseableIterator<T> wrap(Iterator<T> iterator, Closeable closer)
  {
    return new CloseableIterator<>()
    {
      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          try {
            closer.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return hasNext;
      }

      @Override
      public T next()
      {
        return iterator.next();
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }

    return object.toString();
  }

  /**
   * Checks if an authenticated user has the STATE READ permissions needed to view server information.
   */
  public static void checkStateReadAccessForServers(
      AuthenticationResult authenticationResult,
      AuthorizerMapper authorizerMapper
  )
  {
    final AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException("Insufficient permission to view servers: " + authResult.getErrorMessage());
    }
  }

  /**
   * Returns an iterator over all discoverable Druid nodes in the cluster.
   */
  public static Iterator<DiscoveryDruidNode> getDruidServers(DruidNodeDiscoveryProvider druidNodeDiscoveryProvider)
  {
    return Arrays.stream(NodeRole.values())
                 .flatMap(nodeRole -> druidNodeDiscoveryProvider.getForNodeRole(nodeRole).getAllNodes().stream())
                 .collect(Collectors.toList())
                 .iterator();
  }

  /**
   * This table contains currently running and recently completed queries from all SQL engines.
   * Enabled based on {@link PlannerConfig#isEnableSysQueriesTable()}.
   */
  static class QueriesTable extends AbstractTable implements ProjectableFilterableTable
  {
    private final Provider<SqlEngineRegistry> sqlEngineRegistryProvider;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final AuthenticationResult authenticationResult;

    public QueriesTable(
        final Provider<SqlEngineRegistry> sqlEngineRegistryProvider,
        final ObjectMapper jsonMapper,
        final AuthorizerMapper authorizerMapper,
        final AuthenticationResult authenticationResult
    )
    {
      this.sqlEngineRegistryProvider = sqlEngineRegistryProvider;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
      this.authenticationResult = authenticationResult;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(QUERIES_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(
        final DataContext root,
        final List<RexNode> filters,
        @Nullable final int[] projects
    )
    {
      // Check STATE READ authorization
      final AuthorizationResult stateReadAuthorization = AuthorizationUtils.authorizeAllResourceActions(
          authenticationResult,
          Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
          authorizerMapper
      );

      // Get queries from all engines
      final List<QueryInfo> allQueries = new ArrayList<>();
      for (final SqlEngine sqlEngine : sqlEngineRegistryProvider.get().getAllEngines()) {
        final GetQueriesResponse response = sqlEngine.getQueries(
            false, // selfOnly false to get queries from all servers
            true, // includeComplete true to include all queries
            authenticationResult,
            stateReadAuthorization
        );
        allQueries.addAll(response.getQueries());
      }

      // Determine if we need to serialize the info field (based on projection pushdown)
      final int[] nonNullProjects = projects == null ? QUERIES_PROJECT_ALL : projects;
      final boolean includeInfo = containsIndex(nonNullProjects, QUERIES_INFO_INDEX);

      // Build rows
      final FluentIterable<Object[]> results = FluentIterable
          .from(allQueries)
          .transform(queryInfo -> buildQueryRow(queryInfo, includeInfo, jsonMapper))
          .transform(row -> projectQueriesRow(row, nonNullProjects));

      return Linq4j.asEnumerable(results);
    }

    /**
     * Build a full row for a query.
     */
    private static Object[] buildQueryRow(
        final QueryInfo queryInfo,
        final boolean includeInfo,
        final ObjectMapper jsonMapper
    )
    {
      final Object[] row = new Object[QUERIES_SIGNATURE.size()];
      row[0] = queryInfo.executionId();
      row[1] = queryInfo.engine();
      row[2] = queryInfo.state();

      // Only serialize info if it's in the projection
      if (includeInfo) {
        try {
          row[3] = jsonMapper.writeValueAsString(queryInfo);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        row[3] = null;
      }

      return row;
    }

    /**
     * Project a row to include only the columns in the projection.
     */
    private static Object[] projectQueriesRow(final Object[] row, final int[] projects)
    {
      final Object[] projectedRow = new Object[projects.length];
      for (int i = 0; i < projects.length; i++) {
        projectedRow[i] = row[projects[i]];
      }
      return projectedRow;
    }

    /**
     * Check if an array contains a specific index.
     */
    private static boolean containsIndex(final int[] array, final int index)
    {
      for (final int i : array) {
        if (i == index) {
          return true;
        }
      }
      return false;
    }
  }
}
