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

# JSON value-only ingestion: follow-up investigation

Scope: JSON value parsing without Kafka key or header parsing, focusing on explicit dimensions. The comparison baseline is PR #202, including direct byte-array parsing. The retained change is included in PR #202; the token-parser prototype remains experimental.

## Avoid unused dimension discovery

`MapInputRowParser.parse` evaluated `theMap.keySet()` before calling `findDimensions`, even when a nonempty fixed dimension list made the field set irrelevant. For a JSON flattener, `keySet()` scans the tree's root fields and builds a `LinkedHashSet`. This work scales with message width even when only a few dimensions are ingested.

The implementation checks the existing `DimensionsSpec.hasFixedDimensions()` predicate first and directly uses the configured dimension names. Automatic discovery, `includeAllDimensions`, and an empty configured dimension list retain the previous path. It does not remove fields from the event: timestamp extraction, metrics, transforms, and raw field access retain their original values. This optimization applies to both plain JSON input and the Kafka input-format wrapper, and to other map-based formats.

Validation: 38 focused tests passed (`MapInputRowParserTest`, `JsonReaderTest`, `JsonNodeReaderTest`, `JsonReaderBufferTest`). The new regression uses a lazy map that supports field lookup but rejects enumeration, proving that fixed-dimension parsing does not require a root-field scan. Existing tests cover explicit dimensions, automatic discovery, include-all dimensions, and timestamp failures. Processing Checkstyle and production/test forbidden-API checks passed.

## Measured field-scan improvement

JMH 1.37 / Temurin JDK25, two forks, three one-second warmups and four one-second measurements per fork, GC profiler. All timed runs were performed sequentially by GPT-5.6 Luna (maximum reasoning). The benchmark reuses a real `JsonInputFormat` reader and a `ByteEntity` with the PR's direct byte-array parser, produces one Druid row, and consumes selected field values. Before/after variants differ only in `MapInputRowParser`. Timestamp extraction uses a missing-column default, so these timings do not include timestamp-string parsing, Kafka fetching/metadata blending, transforms, or indexing.

| Explicit-schema workload | Baseline ns/op | Fixed-dimension guard ns/op | Time reduction | Allocation before → after |
| --- | --- | --- | --- | --- |
| 256 bytes / 6 fields, all 6 consumed | 898.0 ± 9.7 | 789.7 ± 10.9 | 12.1% | 2,480 → 2,024 B/op |
| 4,096 bytes / 70 fields, 6 configured and consumed | 11,746.3 ± 82.3 | 9,501.2 ± 759.0 | 19.1% | 16,024 → 11,952 B/op |

Errors are JMH 99.9% confidence-interval half-widths. Allocation fell by 18.4% and 25.4%, respectively. Synthetic message shapes are not a claim about production throughput.

The automatic-discovery control (256 bytes) measured 983.7 ± 7.9 → 1,038.2 ± 109.5 ns/op: intervals overlap, so there is no conclusive timing change. Allocation increased from 2,968 to 3,000 B/op (+32 bytes, 1.1%). The change is retained locally for the user's explicit-dimension workload; it is not represented as a universal improvement.

## Larger candidate: selective JSON materialization

The current reader builds a complete `JsonNode` tree and then converts requested fields lazily through `JSONFlattenerMaker`. A selective parser could avoid creating nodes for unused fields while retaining the established conversion behavior for fields that are kept.

Druid already creates `InputRowSchema.columnsFilter` with timestamp, transform/filter, dimension, and aggregator input dependencies. Its contract applies **after flattening**. It cannot be applied directly to raw JSON field names when JSONPath, jq, tree extraction, or renamed root fields introduce other dependencies.

A production implementation must preserve:

- Numeric conversion: integer/long nodes become `Long`; other numeric nodes become `Double`, including integers outside the long range.
- String handling: invalid Unicode surrogates are normalized by the current flattener.
- Dimension discovery rules: nested fields, null columns, and arrays affect discovered dimensions.
- Duplicate fields, encoding detection, malformed records, parser constraints, and multi-record messages.
- Complete raw rows for sampling, even if normal ingestion prunes unused values.
- Timestamp, metric, transform, filter, and flattening dependencies.

A direct `Map` deserializer is not a drop-in replacement: Jackson's default numeric types and eager nested-value construction differ from the current lazy flattener. Experimental speedups must be distinguished from a verified compatible implementation.

## Experimental materialization results

The token prototype parses selected flat scalar fields into a map and calls `MapInputRowParser`. The `token-map-normalized` variant also uses Druid's existing string conversion, including Unicode normalization. It still omits the production intermediate-row/list/iterator wrapper, supports a narrower input contract, and selects fields from the synthetic explicit dimension list rather than deriving real timestamp/transform/filter/metric dependencies. It is not enabled in production code.

| Workload | Improved tree path ns/op | Normalized token prototype ns/op | Time reduction | Allocation before → after |
| --- | --- | --- | --- | --- |
| 256 bytes / compact | 789.7 ± 10.9 | 856.2 ± 4.0 | -8.4% | 2,024 → 1,472 B/op |
| 4,096 bytes / 70 fields, 6 selected | 9,501.2 ± 759.0 | 6,383.9 ± 294.6 | 32.8% | 11,952 → 4,560 B/op |

The compact case is 8.4% slower despite allocating less. The wide case is 32.8% faster with 61.8% less allocation. This suggests investigating an explicitly scoped path for wide, selective records; it does not justify replacing the general reader.

Two less compatible controls were also measured. Their speedups must not be attributed solely to avoiding trees: both omit production row wrappers, plain Jackson maps differ in numeric types, and unnormalized token maps omit Druid's string conversion. In the 4 KiB case, adding string normalization increased token-prototype time from 3.86 to 6.38 µs/op, demonstrating why the control matters.

| Workload | Plain Jackson map ns/op | Allocation B/op | Token map without string normalization ns/op | Allocation B/op |
| --- | --- | --- | --- | --- |
| 256 bytes / compact | 474.4 ± 3.4 | 1,696 | 434.6 ± 26.5 | 1,472 |
| 4,096 bytes / wide | 4,769.3 ± 88.5 | 11,160 | 3,857.1 ± 34.0 | 4,560 |

Outside timing, 48 synthetic fixture combinations passed on each of the baseline and optimized parser classpaths. Validation checks dimensions, selected values, payload contents, and `Long`/`Double` numeric classes for tree/token paths. This validates the fixtures, not the unsupported production cases listed above. The final strengthened regression run passed all 38 parsing tests. The archived preparation script's compile-only run and shell syntax checks also passed.

## Artifacts and reproduction

- [Benchmark source](../benchmarks/src/test/java/org/apache/druid/benchmark/JsonValueMaterializationBenchmark.java)
- [Raw timed results and scripts](kafka-json-value-experiments/)
- [Preparation script](kafka-json-value-experiments/prepare.sh)
- [JMH runner](kafka-json-value-experiments/run.sh)

Preparation uses an existing `benchmarks/target/benchmarks.jar` as the dependency bundle and compiles current production overrides ahead of it. Baseline `MapInputRowParser` is pinned to PR commit `786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6`; its source matches the benchmark's original baseline. Set `MAIN_ROOT` or `FAT_JAR` if the dependency jar is in another checkout.

```sh
MAIN_ROOT=/Users/frankchen/source/open/druid bash dev/kafka-json-value-experiments/prepare.sh

# Current implementation, compact explicit schema.
MAIN_ROOT=/Users/frankchen/source/open/druid \
RESULT_FILE=/tmp/json-current-compact.json \
bash dev/kafka-json-value-experiments/run.sh \
  -p materializer=tree-flattener -p dimensionMode=explicit \
  -p recordShape=compact-all -p messageSize=256

# Same case with the baseline parser override.
MAIN_ROOT=/Users/frankchen/source/open/druid \
PARSER_CLASSES="$PWD/dev/kafka-json-value-experiments/build/baseline-parser-classes" \
RESULT_FILE=/tmp/json-baseline-compact.json \
bash dev/kafka-json-value-experiments/run.sh \
  -p materializer=tree-flattener -p dimensionMode=explicit \
  -p recordShape=compact-all -p messageSize=256
```

For the wide case use `-p recordShape=wide-all -p messageSize=4096`. For the string-normalizing prototype use `-p materializer=token-map-normalized`. Discovery was tested only for the compact tree baseline/current pair. The full parameter cross-product and JFR CPU profiling were not run.

The retained production changes, regression tests and benchmark artifacts are included in PR #202. Full ingestion throughput remains unmeasured.
