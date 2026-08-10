<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Native query execution for component-owned system tables: MVP design

Status: Implemented prototype

## Summary

The current Bindable system tables fetch rows from components and perform SQL operations on the Broker. This MVP keeps
Calcite on the Broker and adds a shared `/druid/v2/system` endpoint to every Druid component. Component-local providers
resolve authorized rows there. The Broker fans out scan-only requests for component-owned tables, concatenates the
provider rows, and executes the complete native query locally.

The implementation uses a generic native-system-query endpoint and a registry of component-owned table providers.
`sys.tasks` and `sys.server_properties` are the first providers. The endpoint currently accepts native GroupBy, Scan,
and Window operator queries, which covers aggregation as well as the web console's computed-status and arbitrary-sort
query.

## Goals

- Represent `sys.tasks` and `sys.server_properties` as native Druid datasources when selected by query context.
- Execute supported native query trees on the Broker over rows returned by component-local providers.
- Make `sys.server_properties` available from every discovered Coordinator, Overlord, Broker, Historical, Indexer,
  MiddleManager, Peon, and Router process.
- Keep `sys.tasks` owned by Overlord while using the same endpoint and provider framework.
- Preserve the existing system-table row signatures and authorization behavior.
- Make future component-owned system tables incremental provider registrations rather than new endpoints and clients.
- Leave the existing Bindable implementation available as the default planning path and avoid adding query-engine
  dependencies to Overlord.
- Prove aggregation and the real web-console query with focused embedded end-to-end tests.

## Non-goals

- General distributed Calcite execution.
- Cross-component joins or distributed aggregation inside component processes.
- Storage-level pushdown for predicates that cannot be represented safely by the task metadata schema, or for limits.
- Every Druid native query type.
- Production hardening of cancellation, streaming, response contexts, or dedicated processing limits.

## Architecture

```text
SQL client
  -> Broker Calcite planner
  -> native Query(SystemTableDataSource(table))
  -> NativeQueryMaker
  -> NativeSystemQueryClient fanout scan-only POST /druid/v2/system to discovered components
  -> component-local provider storage lookup with supported native predicates
  -> internal-caller and original-user authorization
  -> NativeSystemQueryResponse(provider signature, rows)
  -> Broker InlineDataSource
  -> Broker QueryLifecycle/native query engine
  -> SQL result coercion and formatting
```

### Native datasource and Calcite table

`SystemTableDataSource` is a non-cacheable, non-processable leaf datasource in `processing`. Its JSON form is:

```json
{"type":"systemTable","table":"tasks"}
```

Marking it non-processable tells Calcite that arbitrary Scan sorting must be represented as an operator query. For the
web-console query, Calcite produces a `WindowOperatorQuery` with Scan leaf operators and native sort operators instead
of rejecting its non-time ordering.

`SystemSchema` always registers the existing Bindable implementations so that the traditional behavior remains
available. For a query whose context contains `enableNativeQueryForSystemTables=true`, `QueryHandler` removes the
supported system tables from the Bindable plan and `DruidTableScanRule` supplies their native representations
(`NativeTasksTable` and `NativeServerPropertiesTable`). When the context key is absent or false, the existing Bindable
tables are used. The context key selects planning for one query; it is not a global planner property.

For example, a client opts in with:

```json
{"enableNativeQueryForSystemTables": true}
```

### Generic Broker dispatch

`NativeQueryMaker` recognizes a top-level native query over `SystemTableDataSource`, assigns query identifiers, and adds
the original SQL user's authentication information to reserved context keys after user context processing. It treats
all native system tables identically: `NativeSystemQueryClient` sends a scan-only copy to every discovered component.
Components that do not own the requested table return HTTP 501 and are skipped; another transport error fails the
query. The scan-only context asks the endpoint for provider rows while retaining the native
filter for residual evaluation on the Broker. The Broker then runs the original query over the merged
`InlineDataSource`, including for `sys.tasks`; task ownership remains Overlord-side because only Overlord registers the
task provider.

The endpoint returns a `NativeSystemQueryResponse` containing the query toolchest's array signature and rows. This
keeps transport independent of native result classes such as `ResultRow`, `ScanResultValue`, and `RowsAndColumns`.

### Provider registry and component execution

`NativeSystemTableDataSupplier` supplies a row signature, declarative filter-pushdown rules, and authorized rows for one
system table. `NativeSystemQueryModule` always registers the shared endpoint and the `server_properties` provider on
every component. It registers the `tasks` provider only on Overlord, which remains the owner of task metadata. A
future component-owned table adds a provider and one map binding; it does not require a new Jersey resource, RPC
method, Overlord client method, or Broker query-maker branch.

The generic `NativeSystemTableFilterExtractor` continues to use Druid's existing `DimFilter` model. It walks top-level
`AND` conjuncts in Scan and GroupBy filters and in Window Scan leaves, selects conjuncts accepted by the provider's
rules, and passes the original `DimFilter` objects to the provider. It does not introduce another filter AST, descend
into `OR` or `NOT`, or remove anything from the native query. The complete native filter therefore remains the
correctness filter after row materialization.

`NativeTasksTableSupplier` declares rules for exact string values (equality and `IN`) on `task_id`, `group_id`, `type`,
`datasource`, `created_time`, and `status`, plus lexicographic ranges on `created_time`. It translates the extracted
native filters into `TaskStorageQueryFilter`, which continues through `TaskQueryTool` and `MetadataTaskStorage` to
parameterized task-table SQL for active and completed lookups, including MySQL, PostgreSQL, and Derby.

The task metadata columns provide the following safe prefilters:

| System-table column | Metadata action |
|---|---|
| `task_id` | `id = ...` or `id IN (...)` |
| `group_id` | `group_id = ...` or `IN (...)`, retaining `NULL` rows until task-column migration is complete |
| `type` | `type = ...` or `IN (...)`, retaining `NULL` rows until task-column migration is complete |
| `datasource` | `datasource = ...` or `IN (...)` |
| `created_time` | exact values and lower/upper bounds on `created_date` |
| `status` | selects active lookup for `RUNNING`, completed lookup for `SUCCESS`/`FAILED`, or both |

Exact `SUCCESS` versus `FAILED` remains residual because it is stored in the serialized status payload rather than a
portable indexed column. Runner-derived fields and payload-derived fields also remain residual. Every extracted native
filter stays on the native query, so storage pushdown is an optimization and the Broker remains the final correctness
filter. This is storage-level pushdown rather than merely applying the filter after all task rows reach the Overlord.

`NativeServerPropertiesTableSupplier` declares exact string rules for `server` and `service_name`. It applies those
constraints to its own node, reads the component's injected `Properties`, applies the same hidden-property filtering as
`/status/properties`, and reports its own node roles. The Broker discovers all node roles, deduplicates nodes by host
and port, and fans out the same filtered request. Property and node-role predicates remain residual native filters
after provider rows are materialized. The provider performs the same `STATE READ` authorization check as the existing
Bindable table before exposing server information. This avoids a loopback HTTP request for every component while
preserving the established property visibility rules.

The standard `/druid/v2` endpoint remains the segment/query-lifecycle endpoint. `/druid/v2/system` is a sibling native
endpoint because a system table first needs a provider-specific row lookup. Unlike the initial prototype's
Overlord-only endpoint, the shared endpoint is registered on every component. Router explicitly reserves this path for
its local provider instead of forwarding it to a Broker, preventing a fanout loop.

The generic endpoint accepts scan-only row requests carrying a `GroupByQuery`, `ScanQuery`, or `WindowOperatorQuery`
with a directly registered `SystemTableDataSource`. All components have identical endpoint semantics; complete native
query execution always occurs on the Broker. For an accepted request the endpoint:

1. selects the provider from the datasource table name;
2. reconstructs the original SQL authentication result from reserved context fields;
3. asks the provider for rows authorized for both the internal caller and original SQL user;
4. returns the provider signature and rows directly.

A request without the internal scan-only context is rejected. In particular, Overlord does not execute full native
queries locally. This prevents a direct `sys.server_properties` request from accidentally producing a result over only
the Overlord's local properties instead of the complete distributed table.

Authorization occurs before native filtering, expressions, sorting, and aggregation.

## Supported query examples

The aggregation proof for `sys.tasks` executes through the Overlord-owned provider:

```sql
SELECT datasource, COUNT(*)
FROM sys.tasks
WHERE datasource = 'native_sys_a' AND task_id = 'native_sys_mvp_a_0'
GROUP BY datasource
```

The web-console Tasks query is planned as native Scan leaf operators plus a Broker-side Window operator sort after the
Overlord provider returns task rows. It includes a CTE, computed status, and ordering by both a computed priority and
`created_time`.

## Per-query selection

The component endpoint and suppliers are always registered. The native planner selection defaults to the traditional
Bindable path when `enableNativeQueryForSystemTables` is absent or false. Setting it to true on one SQL request selects
the native path for that request. There is no server-level `druid.nativeSystemQueries.enabled` property or legacy
enablement alias.

## Metrics

Each component endpoint emits table-specific metrics for rows it supplies:

- `query/systemTasks/rowsRead`: authorized provider rows supplied by Overlord; and
- `query/systemTasks/rowsReturned`: task rows returned by the Overlord provider endpoint;
- `query/systemServerProperties/rowsRead`: authorized server-property rows supplied to the native engine; and
- `query/systemServerProperties/rowsReturned`: rows returned by that component (the Broker performs the final
  aggregation for fanout queries).

## Simplified end-to-end tests

`NativeSysTasksQueryTest` starts embedded Derby, ZooKeeper, Coordinator, Indexer, Overlord, and Broker services. Each
native SQL request supplies
`enableNativeQueryForSystemTables=true` in its query context. It creates five `NoopTask` records: two for
`native_sys_a` and three for `native_sys_b`.

The test class contains two cases:

1. A filtered GroupBy combines `datasource = 'native_sys_a'` with `task_id = 'native_sys_mvp_a_0'`. It asserts
   `native_sys_a,1`, `rowsRead = 1`, and `rowsReturned = 1`. Reading only the matching record proves that both
   constraints reached task metadata storage; the correct grouped result proves end-to-end native execution.
2. The exact web-console CTE/computed-status/two-key-sort SQL asserts all five task IDs and Overlord provider metrics
   `rowsRead = 5` and `rowsReturned = 5`.

Together they prove SQL planning, query serialization, generic RPC routing, provider lookup, task metadata filter
pushdown, residual native filtering, authorization, task-row adaptation, native aggregation, native Scan/operator
sorting, canonical result transport, and SQL formatting.

`NativeSysServerPropertiesQueryTest` starts embedded Coordinator, Overlord, Broker, Historical, Indexer, and Router
services. It adds one unique property to each service and groups a Broker SQL query over those six properties with the
same native-planning query context. The
expected six rows (including the Broker's custom service name) prove component registration, discovery fanout, Router's
local endpoint handling, local property collection, provider authorization, Broker-side native aggregation, and
residual property filtering. The test intentionally uses one query and no direct endpoint mocks; a missing component
response changes the grouped result and fails the test.

## Production improvements

The MVP proves that the Broker can plan the two supported system tables as native queries, that Overlord can provide
task-owned rows with metadata pushdown, and that the Broker can fan out and aggregate provider rows. It does not yet use the execution-memory or
wire-transport model required for
a general production feature. Production work must improve both areas rather than compensate with a larger Broker
heap, larger fixed frame, redirect retries, or partial results.

### Bounded multi-frame Window execution

The frame used by this MVP is a local, column-oriented `RowsAndColumns` intermediate. A Scan operator materializes
filters, projections, and virtual columns such as the web console's computed task status into this frame before the
downstream Window operators sort or otherwise process the rows. This frame is not the HTTP response and is not sent
from the components to the Broker.

The MVP materializes the complete Scan output into one frame using the existing Broker-side Window execution path and
its existing allocator capacity. If the frame fills, execution fails explicitly; it never returns the rows that
happened to fit.

Production execution should:

- emit multiple bounded frames rather than requiring the complete Scan output to fit in one frame;
- preserve global sorting, partitioning, and Window semantics across frame boundaries;
- use spillable or explicitly memory-bounded processing for operators that require all rows;
- throw a specific row-too-large error when one row cannot fit in a frame;
- account for total operator memory independently from the size of an individual frame; and
- continue to fail explicitly rather than truncate when any resource limit is exceeded.

After multi-frame execution exists, system-table queries should use the established `maxFrameSize` query-context key,
with a default of `4_000_000` bytes for this execution path. The component must also enforce an administrator-controlled
maximum so an untrusted query cannot request an unsafe allocation. The effective value should be:

```text
min(query-context maxFrameSize, component-configured maximum)
```

At that point, `maxFrameSize` will have its normal meaning: the maximum size of each frame, rather than the maximum size
of the complete result.

### Standard native-query wire transport

Normal Broker-to-Historical native queries use HTTP with Jackson Smile, stream results into a `Sequence`, and integrate
with native-query response context, backpressure, byte limits, timeout, and cancellation. The MVP endpoint instead
returns a fully buffered JSON `NativeSystemQueryResponse` containing both the result signature and all result rows.

Production component-native-query transport should converge on the standard native-query behavior:

- accept and produce Jackson Smile for internal requests;
- serialize and deserialize results incrementally rather than buffering a complete response on either component;
- expose results to the Broker as a lazy `Sequence`;
- apply backpressure and `maxScatterGatherBytes` accounting;
- propagate query deadlines, cancellation, and response context;
- use standard native-query error serialization, lifecycle metrics, and logging; and
- close the component-side execution promptly when the Broker stops consuming the response.

The Broker can derive the result signature from the query toolchest, so production transport should not require the
custom `{signature, rows}` wrapper. This wire-format change is independent of multi-frame local execution: frames are
local execution intermediates, while Smile is the Broker-to-component wire encoding.

### Per-query selection

The `enableNativeQueryForSystemTables` query-context key selects native system-table planning for an individual SQL
query. It defaults to the traditional Bindable path. The row endpoint and suppliers are infrastructure registered on
all relevant components and do not have a second server-level enablement switch.

The MVP keeps a hybrid registration and chooses between the following paths during planning:

```text
enableNativeQueryForSystemTables = false
  -> existing Broker-side ScannableTable execution

enableNativeQueryForSystemTables = true
  -> DruidTable -> SystemTableDataSource -> component row fanout -> Broker native execution
```

Disabling native execution for a query selects the existing implementation during planning; it does not first produce a
native plan and then fail during dispatch.

### General component framework

Before enabling additional system tables in production, the generic framework should provide:

- provider registries and service-aware routing for each component that owns system-table data;
- a strict allowlist of supported datasource and query types;
- authorization of both the internal component caller and the original SQL user;
- standard query lifecycle, cancellation, timeout, metrics, logging, and resource accounting;
- typed provider-level predicate and limit pushdown into component storage APIs where supported; and
- rolling-upgrade behavior and compatibility tests when Brokers and owning components run different versions.

## Follow-up work

- Add provider registries for additional component-owned system tables and extend service-aware routing beyond the
  current `server_properties` fanout.
- Extend typed task-metadata pushdown to additional portable predicate shapes without translating unsupported
  predicates or removing the native residual filter.
- Implement the production multi-frame execution and native wire-transport requirements above.
- Add authorization-matrix and rolling-upgrade tests before production enablement.
