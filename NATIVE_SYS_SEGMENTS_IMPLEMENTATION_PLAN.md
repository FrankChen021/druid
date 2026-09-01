# Native `sys.segments` implementation plan

## Base and scope

- Base this work on Apache Druid PR 20183 head `8035122b2a3ab3f60be8aef4e8d164454c1fe266`.
- Add native-query support for `sys.segments`; do not change `sys.server_segments`.
- Keep the existing Bindable path and row contract backward compatible.
- Keep native execution opt-in through `useNativeQueryForSystemTables`.
- Execute native aggregations, expressions, sorting, and residual filters on the Broker. The provider supplies rows through the existing component-local Scan transport.

## Design constraints

1. The Broker that receives and plans the SQL query must execute the `sys.segments` provider locally.
2. Do not discover, contact, or fan out to other Broker nodes.
3. Register the provider only on Brokers.
4. Preserve the current `sys.segments` semantics, which combine Coordinator-derived segment status with the executing Broker's segment metadata cache.
5. Preserve datasource `READ` row authorization for the original user.
6. The native row values must match the descriptor's `RowSignature` exactly. Complex columns declared as strings must use the same JSON serialization as the Bindable path:
   - `shard_spec`
   - `dimensions`
   - `metrics`
   - `projections`
   - `last_compaction_state`

## Implementation steps

1. Add a shared `SegmentsTableDescriptor` containing the table name, existing 20-column signature, Broker ownership, local-only routing, and datasource `READ` row authorization.
2. Add the smallest generic local-only routing capability to the native system-table framework. `SystemTableQueryClient` must invoke the raw local `SystemTableQueryHandler` directly for this mode without `SystemTableNodeLocator`, service discovery, HTTP, or retries to another Broker.
3. Add a Broker-local `SegmentsTableDataProvider` using `BrokerSegmentMetadataCache`, `MetadataSegmentView`, and `ObjectMapper`.
4. Preserve all existing row semantics: published/available merging, segment-ID deduplication, row-count precedence, replica and availability calculations, realtime/active/published/overshadowed flags, replication-factor fallback, and JSON serialization.
5. Advertise safe `datasource` equality/IN pushdown. Keep the original native filter in the Broker query as the correctness-preserving residual filter.
6. Share row construction and serialization with the Bindable implementation so native and Bindable output cannot drift. Avoid an unrelated refactor or a speculative abstraction.
7. Bind the provider only in `CliBroker`, and register the descriptor with the existing native system-table framework.
8. Make `SegmentsTable` implement `NativeSystemTable` and expose a `SystemTableDataSource("segments")`-backed `DruidTable`.
9. Update the native system-table documentation to list `sys.segments`, its local Broker source, supported datasource pushdown, and Broker-side execution behavior.
10. Add an optional generic provider capability that converts only framework-authorized rows into a query-local datasource. Implement `sys.segments` with a batched column-oriented cursor for `STRING` and `LONG` projections, retain row-cursor fallback for unsupported query shapes and types, and do not cache user-specific batches.

## Verification

1. Unit-test the descriptor signature and datasource authorization.
2. Unit-test local-only routing and prove it bypasses node discovery and remote clients.
3. Unit-test provider rows for published, unpublished, realtime, unavailable, overshadowed, duplicate, and row-count fallback cases, including complex-column JSON serialization and datasource pushdown.
4. Verify the Bindable path remains unchanged when native execution is disabled.
5. Verify COUPLED and DECOUPLED native planning.
6. Add embedded native SQL tests for representative native functionality, including distinct aggregation, grouping or expressions, nested aggregation, filters, and projections.
7. Include a multiple-Broker test or equivalent routing assertion proving a query is executed only by the SQL-receiving Broker and rows are not multiplied.
8. Run focused `server`, `sql`, `services`, and embedded tests with `-Pskip-static-checks -Dweb.console.skip=true -T1C`, followed by relevant static checks.
9. Review the complete diff against PR head `8035122b2a3ab3f60be8aef4e8d164454c1fe266` and ensure every changed line is required by this feature.
10. Benchmark the exact Web Console datasource-tab SQL over 500,000 segments, validate every result row against Bindable before measurement, and compare Bindable, legacy native-row, and provider-backed batched execution separately.

## Latest 500K benchmark result

JMH configuration: one fork, two 2-second warmups, three 2-second measurements, JDK 25.0.3. Lower is better.

| Path | Average |
|---|---:|
| Bindable | 238.689 ms/op |
| Legacy native row | 335.129 ms/op |
| Benchmark-only batched wrapper | 187.089 ms/op |
| Provider-backed authorized batches | 172.031 ms/op |

The provider-backed path was about 28% faster than Bindable and 49% faster than the legacy native-row path in this run. This is a local microbenchmark, not a cluster-level latency guarantee.

## Explicitly rejected alternatives

- Do not use `ALL_NODES` for the Broker role; it duplicates rows and introduces inconsistent cache snapshots.
- Do not move the provider to the Coordinator; that would lose Broker-cache-derived availability, replica, and row-count behavior.
- Do not query all Brokers and deduplicate afterward; it adds network and merge cost and makes conflicting cache values arbitrary.
- Do not cache columnar batches across queries; authorization and metadata snapshots are request-specific.
- Do not build frames per query for this path; measured frame construction cost exceeded both Bindable and row-native execution.
