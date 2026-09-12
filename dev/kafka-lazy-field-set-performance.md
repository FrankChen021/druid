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

# Kafka wrapper field-set investigation

The user's workload uses explicitly configured dimensions and typically no key/header parsing. The previous fixed-dimension guard in `MapInputRowParser` remains retained. This follow-up targets another field enumeration in `KafkaInputReader.buildBlendedRows`, used by `inputFormat.type: kafka` even without key/header formats. Plain JSON input bypasses this wrapper.

## Candidate

Previously the wrapper eagerly constructed a `HashSet` combining value dimension names and metadata/header names for every row. It passed a set-difference view to `findDimensions`, even when the configured dimension list was already sufficient.

For fixed dimensions the candidate directly reuses the configured list and defers constructing the combined key set until a caller requests `keySet()` or `entrySet()`. The combined set retains the existing value-first HashSet insertion behavior. Raw value lookup, null fallback, timestamps, metadata boxing, and the metadata map are unchanged. Sampling and discovery retain eager enumeration. The deferred set is stored in a volatile field for publication across threads; no thread-local state is added.

## Validation

Final Checkstyle and production/test forbidden-API checks passed. The archived runner also passed compilation and baseline/candidate fixture validation. The Kafka input-format suite passed 16 cases, including explicit and discovered dimensions, null/header fallback, colliding field names, sampling, and delayed enumeration after advancing the reader to another record.

## Next area identified by source inspection

`IncrementalIndex.toIncrementalIndexRow` builds a HashSet of all known dimensions per row and removes each encountered dimension to track sparse columns. This is a separate downstream candidate that also applies to plain JSON ingestion. Any replacement must preserve sparse-column marking, dynamically discovered columns, null values, and duplicate-dimension handling. It has not been changed or benchmarked in this follow-up; no speedup is claimed for it.

## Benchmark results

Only `KafkaInputReader` differs between baseline and candidate. Both use the current fixed-dimension `MapInputRowParser` guard, timestamp fast path, and PR direct-array JSON readers. All cases have alternating canonical ISO millisecond timestamps, no key/header formats, and consume every configured value dimension. The metadata case additionally reads all four metadata fields four times. Each operation creates a fresh KafkaRecordEntity and calls the actual Kafka input-format reader.

| Workload | Baseline ns/op | Candidate ns/op | Less time | Allocation B/op before → after |
| --- | --- | --- | --- | --- |
| 256 bytes, explicit | 1,104.09 ± 46.69 | 989.18 ± 45.52 | 10.41% | 3,336 → 2,784 |
| 4 KiB, explicit | 5,301.39 ± 58.13 | 5,161.85 ± 122.35 | 2.63% | 13,504 → 12,936 |
| 256 bytes, explicit, repeated metadata | 1,257.98 ± 18.32 | 1,114.14 ± 9.47 | 11.43% | 3,344 → 2,784 |
| 256 bytes, discovery | 1,819.37 ± 6.16 | 1,800.88 ± 11.15 | 1.02% | 5,432 → 5,408 |

Retain the candidate: compact explicit reads improve by about 10.4%, and repeated metadata reads by 11.4%. The 4 KiB timing intervals overlap, so its nominal 2.6% reduction is inconclusive. Explicit reads save 552–568 B/op. Discovery has a small observed improvement, not a reason to generalize the explicit-schema gain to that mode. JIT allocation elimination can account for small differences in measured bytes between workloads.

GPT-5.6 Luna(max) ran JMH 1.37 on Temurin JDK25 with one thread, two forks, three one-second warmups and four one-second measurements per fork, using the GC profiler. Baseline and candidate were run consecutively per workload. Errors are JMH 99.9% confidence interval half-widths. No Maven validation builds ran during the eight accepted measurements. These synthetic parsing/row-consumption measurements do not establish broker-to-index throughput gains.

## Reproduction and status

[Raw JSON, validation helper, and runner](kafka-lazy-field-set-experiments/). The baseline KafkaInputReader is pinned to PR commit `786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6`; common sources and the candidate come from the working tree. Run from the PR checkout with its previous optimizations present:

```sh
DRUID_DEPENDENCY_ROOT=/Users/frankchen/source/open/druid COMPILE_ONLY=true \
  bash dev/kafka-lazy-field-set-experiments/run-kafka-lazy-fields-benchmark.sh

# Omit COMPILE_ONLY to reproduce the eight timed cases.
```

The retained candidate, regression coverage and benchmark artifacts are included in PR #202.
