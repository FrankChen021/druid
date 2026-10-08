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

package org.apache.druid.msq.dart.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DartControllerContextTest
{
  private static final List<DruidServerMetadata> SERVERS = ImmutableList.of(
      new DruidServerMetadata("no", "localhost:1001", null, 1, null, ServerType.HISTORICAL, "__default", 2), // plaintext
      new DruidServerMetadata("no", null, "localhost:1002", 1, null, ServerType.HISTORICAL, "__default", 1), // TLS
      new DruidServerMetadata("no", "localhost:1003", null, 1, null, ServerType.REALTIME, "__default", 0)
  );
  private static final DruidNode SELF_NODE = new DruidNode("none", "localhost", false, 8080, -1, true, false);
  private static final String QUERY_ID = "abc";

  /**
   * Context returned by {@link #query}. Overrides "maxConcurrentStages".
   */
  private final QueryContext queryContext =
      QueryContext.of(
          ImmutableMap.of(
              MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, 3,
              QueryContexts.CTX_DART_QUERY_ID, QUERY_ID
          )
      );
  private MemoryIntrospector memoryIntrospector;
  private AutoCloseable mockCloser;

  /**
   * Server view that returns {@link #SERVERS}.
   */
  @Mock
  private BrokerServerView serverView;

  /**
   * Query spec that exists mainly to test {@link DartControllerContext#queryKernelConfig}.
   */
  @Mock
  private LegacyMSQSpec querySpec;

  /**
   * Query returned by {@link #querySpec}.
   */
  @Mock
  private Query query;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    memoryIntrospector = new MemoryIntrospectorImpl(100_000_000, 0.75, 1, 1, null);
    Mockito.when(serverView.getDruidServerMetadatas()).thenReturn(SERVERS);
    Mockito.when(querySpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
    Mockito.when(querySpec.getContext()).thenReturn(queryContext);
    Mockito.when(querySpec.getQuery()).thenReturn(query);
    Mockito.when(query.getDataSource()).thenReturn(new TableDataSource("foo"));
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_queryKernelConfig()
  {
    final DartControllerContext controllerContext = new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        queryContext
    );
    final ControllerQueryKernelConfig queryKernelConfig = controllerContext.queryKernelConfig(querySpec);

    Assertions.assertFalse(queryKernelConfig.isFaultTolerant());
    Assertions.assertFalse(queryKernelConfig.isDurableStorage());
    Assertions.assertEquals(3, queryKernelConfig.getMaxConcurrentStages());
    Assertions.assertEquals(TaskReportMSQDestination.instance(), queryKernelConfig.getDestination());
    Assertions.assertTrue(queryKernelConfig.isPipeline());

    // Check workerIds after sorting, because they've been shuffled.
    Assertions.assertEquals(
        ImmutableList.of(
            // Only the HISTORICAL servers
            WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromDruidServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        queryKernelConfig.getWorkerIds().stream().sorted().collect(Collectors.toList())
    );
  }

  /** All queries default to two concurrent stages; explicit context values override this in both planning modes. */
  @ParameterizedTest
  @CsvSource({
      "false, false, 0, 2",
      "true, false, 0, 2",
      "true, true, 0, 2",
      "true, false, 1, 1",
      "true, true, 1, 1",
      "true, false, 2, 2",
      "true, true, 2, 2"
  })
  public void test_queryKernelConfig_stageLimit(
      final boolean systemTable,
      final boolean preplanned,
      final int configuredStages,
      final int expectedStages
  )
  {
    final QueryContext context = QueryContext.of(
        configuredStages == 0
        ? Map.of(QueryContexts.CTX_DART_QUERY_ID, QUERY_ID)
        : Map.of(
            QueryContexts.CTX_DART_QUERY_ID, QUERY_ID,
            MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, configuredStages
        )
    );
    final MSQSpec spec;
    if (preplanned) {
      final QueryDefMSQSpec preplannedSpec = Mockito.mock(QueryDefMSQSpec.class);
      final QueryDefinition definition = Mockito.mock(QueryDefinition.class);
      final StageDefinition stage = Mockito.mock(StageDefinition.class);
      Mockito.when(preplannedSpec.getContext()).thenReturn(context);
      Mockito.when(preplannedSpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
      Mockito.when(preplannedSpec.getQueryDef()).thenReturn(definition);
      Mockito.when(definition.getStageDefinitions()).thenReturn(List.of(stage));
      Mockito.when(stage.getInputSpecs()).thenReturn(List.of(new SystemTableInputSpec("server_properties")));
      spec = preplannedSpec;
    } else {
      Mockito.when(querySpec.getContext()).thenReturn(context);
      if (systemTable) {
        Mockito.when(query.getDataSource()).thenReturn(new SystemTableDataSource("server_properties"));
      }
      spec = querySpec;
    }
    final ControllerQueryKernelConfig config = new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        context
    ).queryKernelConfig(spec);

    Assertions.assertEquals(expectedStages, config.getMaxConcurrentStages());
    Assertions.assertEquals(expectedStages > 1, config.isPipeline());
    Assertions.assertEquals(
        expectedStages,
        config.getWorkerContextMap().get(MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES)
    );
  }

  /** A system-table query uses the Broker fallback plus only the discovered Historical workers. */
  @Test
  public void test_queryKernelConfig_systemTableUsesBrokerWorker()
  {
    Mockito.when(query.getDataSource()).thenReturn(new SystemTableDataSource("server_properties"));
    final DartControllerContext controllerContext = new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        queryContext
    );

    final List<String> workerIds = controllerContext.queryKernelConfig(querySpec).getWorkerIds();
    Assertions.assertEquals(3, workerIds.size());
    Assertions.assertEquals(WorkerId.fromDruidNode(SELF_NODE, QUERY_ID).toString(), workerIds.get(0));
    Assertions.assertEquals(
        Set.of(
            WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromDruidServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        Set.copyOf(workerIds.subList(1, workerIds.size()))
    );
  }

  /** A coupled preplanned query recognizes its system-table input spec and uses the same distributed worker set. */
  @Test
  public void test_queryKernelConfig_preplannedSystemTableUsesBrokerWorker()
  {
    final QueryDefMSQSpec preplannedQuerySpec = Mockito.mock(QueryDefMSQSpec.class);
    final QueryDefinition queryDefinition = Mockito.mock(QueryDefinition.class);
    final StageDefinition stageDefinition = Mockito.mock(StageDefinition.class);
    Mockito.when(preplannedQuerySpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
    Mockito.when(preplannedQuerySpec.getContext()).thenReturn(queryContext);
    Mockito.when(preplannedQuerySpec.getQueryDef()).thenReturn(queryDefinition);
    Mockito.when(queryDefinition.getStageDefinitions()).thenReturn(List.of(stageDefinition));
    Mockito.when(stageDefinition.getInputSpecs()).thenReturn(
        List.of(new SystemTableInputSpec("server_properties"))
    );

    final DartControllerContext controllerContext = new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        queryContext
    );

    final List<String> workerIds = controllerContext.queryKernelConfig(preplannedQuerySpec).getWorkerIds();
    Assertions.assertEquals(3, workerIds.size());
    Assertions.assertEquals(WorkerId.fromDruidNode(SELF_NODE, QUERY_ID).toString(), workerIds.get(0));
    Assertions.assertEquals(
        Set.of(
            WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromDruidServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        Set.copyOf(workerIds.subList(1, workerIds.size()))
    );
  }

  /** The initial Dart integration rejects mixed system and segment datasources instead of misrouting the latter. */
  @Test
  public void test_queryKernelConfig_mixedSystemAndSegmentDataSourcesAreRejected()
  {
    final DataSource compositeDataSource = Mockito.mock(DataSource.class);
    Mockito.when(compositeDataSource.getChildren()).thenReturn(
        List.of(new SystemTableDataSource("server_properties"), new TableDataSource("foo"))
    );
    Mockito.when(query.getDataSource()).thenReturn(compositeDataSource);
    final DartControllerContext controllerContext = new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        queryContext
    );

    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> controllerContext.queryKernelConfig(querySpec)
    );
    Assertions.assertEquals(
        "Dart system-table queries cannot mix system tables with other datasources",
        exception.getMessage()
    );
  }
}
