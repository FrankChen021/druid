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

# Kafka ingestion performance follow-ups

## Retained optimization: fixed dimensions avoid field discovery

Keep the `MapInputRowParser` guard using `DimensionsSpec.hasFixedDimensions()` to avoid calling the lazy flattened map's `keySet()` when the configured dimension list is already sufficient. The user's workload uses explicit dimensions and normally no Kafka key/header parsing.

Measured against PR #202's JSON reader: 12.1% less parsing time / 18.4% fewer allocated bytes for a 256-byte six-field record; 19.1% less parsing time / 25.4% fewer allocated bytes for a 4 KiB seventy-field record with six selected fields. Discovery control had overlapping timing intervals and an observed 32-byte/op allocation increase. Full results and caveats: [JSON value investigation](kafka-json-value-performance.md).

This change is implemented and regression-tested in the isolated PR worktree, and must remain present in both sides of subsequent comparisons. It is included in PR #202.

## Rejected experiment: metadata snapshot for value-only reads

`KafkaInputReader.read()` still builds a four-entry `HashMap` containing Kafka timestamp, topic, partition, and offset when no key or header parser is configured. The candidate stores primitive numeric metadata plus the topic string in a small immutable snapshot, shares the metadata name mapping between records, and boxes numeric values only when requested.

The snapshot does not retain `ConsumerRecord`, the source entity, the reader, or payload bytes. The name-to-position mapping preserves the existing first-wins precedence when metadata column names coincide, as well as HashMap key iteration order for colliding names. Value fields continue to take precedence; null values still fall back to metadata. Sampling, tombstones, and configured key/header parsers use the prior path.

Regression coverage compares the optimized read path against the unchanged sample path, including distinct names, aliased names, colliding string hashes, numeric Java types, null/value precedence, unknown keys, and retained rows after advancing to another record.

Repeated access to numeric metadata is a tradeoff: the snapshot may box a value more than once, whereas the old HashMap retained one boxed instance per record. Benchmarks therefore need both unused-metadata and metadata-access cases with timestamp/offset values outside the boxed-number caches.

### Metadata snapshot results and decision

Both sides include the retained fixed-dimension guard and PR #202's direct byte-array JSON parser. The harness uses the actual `KafkaInputFormat.read()` path with no key/header parsers, a fresh `KafkaRecordEntity` wrapper per operation, timestamp extraction, and consumption of every configured value dimension. Metadata timestamps and offsets lie outside boxed-number caches. The 4 KiB fixture has 70 value fields plus a timestamp; six value fields are configured. These are synthetic parser/row-consumption benchmarks, not Kafka broker-to-index throughput measurements.

| Workload | Baseline ns/op | Snapshot ns/op | Time reduction | Allocation B/op before → after |
| --- | --- | --- | --- | --- |
| Explicit, 256 bytes, metadata unused | 1,043.69 ± 10.29 | 1,043.45 ± 5.22 | 0.02% | 3,232 → 2,984 |
| Explicit, 256 bytes, metadata read once | 1,096.98 ± 2.99 | 1,105.72 ± 3.99 | -0.80% | 3,232 → 3,032 |
| Explicit, 256 bytes, metadata read four times | 1,214.89 ± 4.80 | 1,297.72 ± 6.37 | -6.82% | 3,232 → 3,176 |
| Explicit, 4 KiB, metadata unused | 5,271.55 ± 37.88 | 5,235.04 ± 37.00 | 0.69% | 13,392 → 13,144 |
| Discovery, 256 bytes, metadata unused | 1,765.69 ± 12.32 | 1,733.33 ± 14.46 | 1.83% | 5,352 → 5,120 |

The explicit-schema unused-metadata timings have overlapping intervals. Repeated metadata reads are 6.82% slower, despite modest allocation savings. The implementation added enough complexity that these results do not justify retaining it. Production and test files for this experiment were restored to PR HEAD; the [candidate source](kafka-wrapper-experiments/candidate/KafkaInputReader.java), [patch including tests](kafka-wrapper-experiments/metadata-snapshot.patch), and raw measurements are retained for reference. The archived patch uses zero context; apply it to its recorded baseline with `git apply --unidiff-zero`. The rejected candidate passed 56 focused regression tests plus Kafka Checkstyle and forbidden-API checks before removal.

## Retained optimization: common UTC timestamp parsing

`TimestampSpec` caches the last parsed timestamp. A repeated constant fixture masks most date-parsing work. A diagnostic using the same baseline classpath measured a 256-byte explicit Kafka read at 1,051.17 ± 5.80 ns/op with a constant timestamp versus 1,791.28 ± 4.83 ns/op with two alternating timestamp values. Allocation rose from 3,232 to 4,272 B/op. This is a workload comparison, not an optimization speedup.

The new `TimestampParser` fast path recognizes exactly `yyyy-MM-ddTHH:mm:ss.SSSZ` for `iso` and `auto`. It reads the numeric components directly and delegates calendar/range validation to the existing Joda ISO UTC chronology, then constructs the usual Druid `DateTime`. It introduces no cache or thread-local state. Other representations (offsets, different fractions, date-only forms, etc.) and invalid inputs fall back to the existing parser. Quoted/trimmed `iso` input still follows the original normalization step.

### Timestamp results

The following comparisons use baseline Kafka metadata handling on both sides; the rejected snapshot is absent. Only `TimestampParser` differs. The direct-array JSON parser and fixed-dimension guard remain common. The ingestion timestamp format in these benchmarks is `iso`; `auto` eligibility is regression-tested but not separately timed. Alternating messages are prepared before timing, differ only in their timestamp milliseconds, and retain the stated byte length.

| Explicit-schema workload | Baseline ns/op | Fast path ns/op | Time reduction | Allocation B/op before → after |
| --- | --- | --- | --- | --- |
| 256 bytes, alternating timestamps | 1,801.52 ± 17.95 | 1,094.50 ± 7.99 | 39.25% | 4,272 → 3,344 |
| 4 KiB, alternating timestamps | 6,111.53 ± 41.14 | 5,268.13 ± 32.20 | 13.80% | 14,432 → 13,504 |
| 256 bytes, constant timestamp control | 1,059.39 ± 5.04 | 1,081.55 ± 125.65 | -2.09% | 3,232 → 3,232 |

The varying-timestamp cases save 928 B/op (21.7% for 256 bytes, 6.4% for 4 KiB). The constant-timestamp control has overlapping intervals and identical allocation, as expected when the existing timestamp cache handles the messages. This optimization applies to eligible timestamps in plain JSON ingestion as well as the Kafka input-format wrapper; it does not require keys or headers.

### Validation and methodology

The final reactor run passed 90 focused tests: 75 processing tests and 15 Kafka input-format tests. Three additional `TimestampSpecTest` cases passed in the earlier focused run against the same timestamp implementation. New timestamp coverage compares calendar boundaries (including year zero, year 9999, leap years, and pre-epoch dates) with the general parser and checks fallback results, exception types, and error messages for invalid fields and other timestamp forms. Processing and benchmark Checkstyle and processing production/test forbidden-API checks passed. The archived timestamp runner passed compile-only reproduction. A separate baseline/current comparison of 24 ISO/auto fixtures produced identical results and errors; outputs are archived under `kafka-wrapper-experiments/timestamp-fastpath/parity/`.

All accepted timed runs were executed by GPT-5.6 Luna with maximum reasoning, using Temurin JDK25, JMH1.37, one thread, two forks, three one-second warmups and four one-second measurements per fork, with the GC profiler. Errors shown are JMH 99.9% confidence-interval half-widths. An early metadata run that loaded a stale JSON reader was discarded; only corrected source-override runs are archived. Timed comparisons did not overlap final validation builds.

### Reproduction

[Benchmark source](../benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatReadBenchmark.java) · [Raw data and scripts](kafka-wrapper-experiments/)

The scripts compile isolated baseline/current production overrides ahead of an existing benchmark fat jar and Kafka extension jar. The baseline is pinned to PR commit `786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6`. They require JDK25 and the local dependency jars; point `DRUID_DEPENDENCY_ROOT` at the checkout containing those jars.

```sh
# Compile only, without running measurements.
DRUID_DEPENDENCY_ROOT=/Users/frankchen/source/open/druid COMPILE_ONLY=true \
  bash dev/kafka-wrapper-experiments/run-timestamp-fastpath-benchmark.sh

# Six timestamp baseline/current cases.
DRUID_DEPENDENCY_ROOT=/Users/frankchen/source/open/druid \
  bash dev/kafka-wrapper-experiments/run-timestamp-fastpath-benchmark.sh

# Reproduce the rejected metadata snapshot comparison from its archived source.
DRUID_DEPENDENCY_ROOT=/Users/frankchen/source/open/druid \
  bash dev/kafka-wrapper-experiments/run-kafka-input-format-read.sh
```

The retained production changes, regression tests and benchmark artifacts are included in PR #202. Full ingestion throughput remains unmeasured.

## Later follow-up

The [Kafka wrapper field-set investigation](kafka-lazy-field-set-performance.md) retains deferred field enumeration for fixed dimensions, measuring about 10.4% less time for compact explicit-schema reads. Its results are incremental to the timestamp and MapInputRowParser changes above.

The [incremental-index dimension-presence investigation](incremental-index-dimension-presence-performance.md) separately measured a boolean-array replacement for per-row HashSet tracking in actual on-heap rollup indexing. It is retained, with parsing excluded from that benchmark.

The [empty parse-error fast path](incremental-index-empty-error-performance.md) is retained as a small allocation improvement: about 56 B per valid row in the indexing benchmark, with inconclusive timing changes.
