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

# Incremental-index dimension presence

## Change

`IncrementalIndex.toIncrementalIndexRow` previously copied all known dimension names into a HashSet for every row, removed encountered names, and marked the remaining dimensions sparse. Dimension descriptors already have dense, stable indexes and are appended to `dimensionDescsList` under the same lock.

The candidate uses a per-row boolean array indexed by dimension number. Existing dimensions are marked present before conversion, preserving the handling of nulls and parse errors. A final indexed scan marks unencountered existing dimensions sparse. Newly discovered dimensions retain the existing first-row/later-row handling. The bounds guard excludes dimensions added during this row from the pre-existing-dimension array; duplicate handling remains in its existing code path.

This removes HashSet nodes, the bucket array, and hash-based insertion/removal/lookups for presence tracking. It does not remove the remaining dimension descriptor lookup used for indexing. The boolean array is local to the row; no cache or thread-local state is introduced. The change applies downstream of Kafka and plain JSON readers, as well as other ingestion sources using IncrementalIndex.

## Validation

The incremental-index package suite completed 104 tests: 103 passed, one skipped, no failures or errors. New regression coverage checks known present columns, omitted columns, first-row discovered dimensions, later discovered dimensions, and explicit nulls. Existing duplicate-dimension, row-size, comparison, ingestion and cursor tests also ran. Processing Checkstyle and production/test forbidden-API checks passed. The archived runner also compiled and validated both variants successfully.

## Benchmark scope

Measure actual on-heap `IncrementalIndex.add` with prebuilt rows and rollup, holding dimension values/cardinality bounded. Dense cases represent rows with all six or all 64 configured dimensions present; the sparse control omits half of 64 dimensions from the row's dimension list. Parsing and Kafka broker I/O are outside timing. Do not combine these percentages arithmetically with the earlier parser benchmarks.

The harness batches 4,096 adds into a fresh index and normalizes JMH output per row using `@OperationsPerInvocation(4096)`. Index setup and teardown validation are outside the timed batch, but the GC profiler includes their allocations amortized across the batch. Allocation values therefore describe the benchmark as a whole per row, rather than allocations inside `add` alone. Each batch rolls up to 32 distinct rows and validates dimension values, omitted fields, and count totals.

## Results and decision

| Workload | Baseline ns/row | Candidate ns/row | Less time | Profiled B/row before → after |
| --- | --- | --- | --- | --- |
| 6 dense dimensions | 643.97 ± 32.58 | 497.01 ± 56.48 | 22.82% | 873.28 → 561.25 |
| 64 dense dimensions | 4,485.44 ± 191.68 | 3,250.60 ± 46.79 | 27.53% | 6,245.34 → 3,685.30 |
| 64 dimensions, half omitted | 2,984.35 ± 215.82 | 1,847.40 ± 37.17 | 38.10% | 4,732.49 → 2,148.47 |

Retain the change: all three timing comparisons show separated confidence intervals and lower allocation. The six-dimension case takes 22.8% less time and profiles 312 fewer bytes per row. The dense 64-dimension case takes 27.5% less time and saves approximately 2,560 B/row; the sparse control takes 38.1% less time and saves approximately 2,584 B/row. These findings are for the bounded on-heap rollup benchmark, not full ingestion or non-rollup/high-cardinality throughput.

GPT-5.6 Luna(max) performed six timed runs using Temurin JDK25, JMH1.37, one thread, two forks, three one-second warmups and four one-second measurements per fork, with the GC profiler. Each baseline/candidate pair ran consecutively. Errors shown are JMH 99.9% confidence-interval half-widths. Parent test/static-check builds finished before timing began. Only IncrementalIndex differs in the measured classpaths; both implementations use the same benchmark fat jar for other classes.

## Reproduction and saved state

[Raw results, source fingerprints, fixture validation, and runner](incremental-index-dimension-presence-experiments/). The baseline source is archived from commit `786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6`. The candidate is loaded from the current PR checkout. The exact measured harness is retained as `results/DimensionPresenceBenchmark.measured.java`; the runnable source adds only final qualifiers to loop variables after timing.

```sh
DRUID_BENCHMARK_FATJAR=/Users/frankchen/source/open/druid/benchmarks/target/benchmarks.jar \
  bash dev/incremental-index-dimension-presence-experiments/run-dimension-presence-benchmark.sh validate

# Use run instead of validate for the six timed cases.
```

The retained production changes, regression tests and benchmark artifacts are included in PR #202. Full ingestion throughput remains unmeasured.
