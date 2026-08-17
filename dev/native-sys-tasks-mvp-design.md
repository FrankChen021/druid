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

The current Bindable system tables fetch rows from components and perform SQL operations on the Broker. This design
keeps Calcite on the Broker and uses the standard `/druid/v2` native-query endpoint on every Druid component.
Component-local suppliers resolve authorized rows, and the normal native query stack applies filters, projections,
and partial aggregation before standard native results return to the Broker for merging and finalization.

The implementation uses a generic datasource query-handler registry rather than a system-specific HTTP resource or
response envelope. `sys.tasks` and `sys.server_properties` are the first suppliers. Supported native GroupBy, Scan,
and Window operator queries therefore use the same endpoint, serialization, response context, timeout, cancellation,
and result-merging machinery as other native datasources.

## Goals

- Represent `sys.tasks` and `sys.server_properties` as native Druid datasources when using the native SQL engine.
- Execute supported native query trees on components and merge their native results on the Broker.
- Make `sys.server_properties` available from every discovered Coordinator, Overlord, Broker, Historical, Indexer,
  MiddleManager, Peon, and Router process.
- Keep `sys.tasks` owned by Overlord while using the same endpoint and provider framework.
- Preserve the existing system-table row signatures and authorization behavior.
- Make future component-owned system tables incremental provider registrations rather than new endpoints and clients.
- Leave the existing Bindable implementation available for system tables without a native representation.
- Prove aggregation and the real web-console query with focused embedded end-to-end tests.

## Non-goals

- General distributed Calcite execution.
- Cross-component joins.
- Storage-level pushdown for predicates that cannot be represented safely by the task metadata schema, or for limits.
- Every Druid native query type.
- A new system-table-specific HTTP protocol or response type.

## Architecture

```text
SQL client
  -> Broker Calcite planner
  -> native Query(SystemTableDataSource(table))
  -> QueryLifecycle datasource handler
  -> NativeSystemQueryClient fanout POST /druid/v2 to discovered components
  -> standard component QueryResource and QueryLifecycle
  -> component-local provider storage lookup with supported native predicates
  -> internal-caller and original-user authorization
  -> component native execution over a local InlineDataSource
  -> standard native partial results
  -> Broker native merge and finalization
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

`SystemSchema` always registers the existing Bindable implementations so that unsupported system tables retain their
traditional behavior. When the resolved SQL engine is `native`, `QueryHandler` removes supported system tables from
the Bindable plan and `DruidTableScanRule` supplies their native representations (`NativeTasksTable` and
`NativeServerPropertiesTable`). The `engine` query-context key therefore selects the SQL engine and table execution
path together. Because Druid's default SQL engine is `native`, no additional context is required for these tables.

### Generic Broker dispatch

`QueryLifecycle` selects a `DataSourceQueryHandler` by datasource class. On the Broker,
`NativeSystemQueryClient` discovers only the node roles registered for the requested table and creates standard
`DirectDruidClient` runners for those components. It adds an internal component-local marker, the original SQL user's
authentication information, a deadline, and distinct component query and resource identifiers. The normal query
factory and toolchest merge component results and finalize them on the Broker.

There is no `NativeQueryMaker` table-name branch, custom HTTP client, or custom response envelope. `NativeQueryMaker`
consumes the final `QueryLifecycle` result exactly as it does for another native datasource.

### Provider registry and component execution

`NativeSystemTableDataSupplier` supplies a row signature, declarative filter-pushdown rules, and authorized rows for one
system table. `NativeSystemQueryModule` registers the `server_properties` supplier on every component and the `tasks`
supplier only on Overlord, which remains the owner of task metadata. A future component-owned table adds a supplier,
routing descriptor, and map binding; it does not require a new Jersey resource, RPC method, client method, or Broker
query-maker branch.

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
filter stays on the native query, so storage pushdown is an optimization and the component native engine remains the
final correctness filter. This is storage-level pushdown rather than merely applying the filter after all task rows
reach the Overlord.

`NativeServerPropertiesTableSupplier` declares exact string rules for `server` and `service_name`. It applies those
constraints to its own node, reads the component's injected `Properties`, applies the same hidden-property filtering as
`/status/properties`, and reports its own node roles. The Broker discovers all node roles, deduplicates nodes by host
and port, and fans out the same filtered request. Property and node-role predicates remain residual native filters
after provider rows are materialized. The provider performs the same `STATE READ` authorization check as the existing
Bindable table before exposing server information. This avoids a loopback HTTP request for every component while
preserving the established property visibility rules.

The standard `/druid/v2` endpoint handles both ordinary and system-table native queries. For a component-local request:

1. `QueryLifecycle` selects the registered system-table handler;
2. reconstructs the original SQL authentication result from reserved context fields;
3. asks the supplier for rows authorized for both the internal caller and original SQL user;
4. replaces the system datasource with a local `InlineDataSource`; and
5. executes the unchanged native query locally and returns its standard result stream.

A component request without both the internal marker and escalated service identity is rejected. The Router recognizes
only such authenticated internal requests for local `/druid/v2` execution; ordinary client requests continue to be
forwarded to a Broker. The Broker handler checks the marker before deciding between local execution and fanout, so a
marked request that reaches a Broker cannot recursively fan out. Component query-id prefixes also keep cancellation
requests local on leader-elected management services.

Authorization occurs before native filtering, expressions, sorting, and aggregation.

## Supported query examples

The aggregation proof for `sys.tasks` executes through the Overlord-owned provider:

```sql
SELECT datasource, COUNT(*)
FROM sys.tasks
WHERE datasource = 'native_sys_a' AND task_id = 'native_sys_mvp_a_0'
GROUP BY datasource
```

The web-console Tasks query is planned as native Scan leaf operators plus Window operators after the Overlord supplier
resolves task rows. It includes a CTE, computed status, and ordering by both a computed priority and `created_time`.

## Engine selection

The component endpoint and suppliers are always registered. With the `native` SQL engine, tables that have a native
representation use the native path and other system tables fall back to Bindable execution. Since `native` is the
default SQL engine, supported system tables use native execution even when the query omits the `engine` context key.
Engines without Bindable support continue to reject unsupported system tables. There is no separate native-system-table
feature flag or server-level enablement property.

## Metrics

Each component handler emits a table-specific metric for rows its supplier reads:

- `query/systemTasks/rowsRead`: authorized task rows supplied by Overlord; and
- `query/systemServerProperties/rowsRead`: authorized server-property rows supplied to the native engine.

Standard query lifecycle metrics describe native execution and returned results; the custom transport-specific
`rowsReturned` metric is removed with the custom endpoint.

## Simplified end-to-end tests

`NativeSysTasksQueryTest` starts embedded Derby, ZooKeeper, Coordinator, Indexer, Overlord, and Broker services. It
creates five `NoopTask` records: two for `native_sys_a` and three for `native_sys_b`. Its SQL requests omit the engine
context, demonstrating that the default `native` engine selects native system-table execution.

The test class contains two cases:

1. A filtered GroupBy combines `datasource = 'native_sys_a'` with `task_id = 'native_sys_mvp_a_0'`. It asserts
   `native_sys_a,1` and `rowsRead = 1`. Reading only the matching record proves that both
   constraints reached task metadata storage; the correct grouped result proves end-to-end native execution.
2. The exact web-console CTE/computed-status/two-key-sort SQL asserts all five task IDs and Overlord supplier metric
   `rowsRead = 5`.

Together they prove SQL planning, standard `/druid/v2` routing, supplier lookup, task metadata filter pushdown, residual
native filtering, authorization, task-row adaptation, native aggregation, native Scan/operator sorting, standard result
transport, and SQL formatting.

`NativeSysServerPropertiesQueryTest` starts embedded Coordinator, Overlord, Broker, Historical, Indexer, and Router
services. It adds one unique property to each service and groups a Broker SQL query over those six properties. The
expected six rows (including the Broker's custom service name) prove component registration, discovery fanout, Router's
safe local endpoint handling, local property collection, supplier authorization, component partial aggregation,
Broker merge, and residual property filtering. The test intentionally uses one query and no direct endpoint mocks; a
missing component response changes the grouped result and fails the test.

## Production improvements

The implementation proves that the Broker can plan the two supported system tables as native queries, that Overlord can
provide task-owned rows with metadata pushdown, and that components can execute native queries before the Broker merges
their standard results. Production work must complete bounded operator execution and resource sizing rather than
compensate with a larger heap, larger fixed frame, redirect retries, or partial results.

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

Broker-to-component requests now use `DirectDruidClient` and `/druid/v2`, including Jackson Smile, lazy `Sequence`
consumption, response context, scatter-gather accounting, deadlines, cancellation identifiers, native error handling,
and query-toolchest result merging. There is no custom `{signature, rows}` wrapper and no fully buffered custom HTTP
response.

Production validation must still cover cancellation and backpressure on every newly query-capable management process,
rolling upgrades, response-size limits, and cleanup when the Broker stops consuming a component response. Frames remain
local execution intermediates; Smile remains the Broker-to-component wire encoding.

### Engine selection

The existing `engine` query-context key selects the SQL execution engine. When its resolved value is `native`, a system
table with a native representation uses native planning. A system table without a native representation remains on the
Bindable path. The row endpoint and suppliers are infrastructure registered on all relevant components and do not have
a second server-level enablement switch.

The MVP keeps a hybrid registration and chooses between the following paths during planning:

```text
engine = native and native table representation exists
  -> DruidTable -> SystemTableDataSource -> component native execution -> Broker merge

engine = native and no native table representation exists
  -> existing Broker-side ScannableTable execution
```

This is a table-capability decision made before native planning. Once a table selects the native path, a later native
planning failure is returned to the caller; the planner does not retry that query with Bindable execution.

### General component framework

Before enabling additional system tables in production, the generic framework should provide:

- provider registries and service-aware routing for each component that owns system-table data;
- a strict allowlist of supported datasource and query types;
- authorization of both the internal component caller and the original SQL user;
- verified query lifecycle, cancellation, timeout, metrics, logging, and resource accounting on every component role;
- typed provider-level predicate and limit pushdown into component storage APIs where supported; and
- rolling-upgrade behavior and compatibility tests when Brokers and owning components run different versions.

## Follow-up work

- Add provider registries for additional component-owned system tables and extend service-aware routing beyond the
  current `server_properties` fanout.
- Extend typed task-metadata pushdown to additional portable predicate shapes without translating unsupported
  predicates or removing the native residual filter.
- Implement production multi-frame execution and validate standard native transport on every component role.
- Add authorization-matrix and rolling-upgrade tests before production enablement.
