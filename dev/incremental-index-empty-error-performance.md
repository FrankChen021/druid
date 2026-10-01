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

# Incremental-index empty-error fast path

## Change

`IncrementalIndex.add` calls `getCombinedParseException` for every row. Previously that method constructed a StringBuilder and a details list, copied both error lists and counted messages before returning null when there were no errors.

The candidate returns null immediately when both the dimension and aggregation error lists are null or empty. This avoids preparing a combined error for valid rows. The error-producing branch remains unchanged, preserving exception type, message formatting, detail order, and partially valid row handling. The upstream per-row error lists themselves remain unchanged; this optimization specifically targets combining their results.

## Validation

The incremental-index suite ran 106 tests: 105 passed and one skipped, with no failures or errors. Added coverage includes the four null/empty combinations, dimension-only errors, aggregation-only errors, and combined error detail/message order. Existing parse-error ingestion tests also passed. Checkstyle and production/test forbidden-API checks passed. The archived runner compiled and validated both source variants successfully.

## Benchmark scope

Reuse the preceding on-heap rollup indexing harness with prebuilt string-valued rows. Each timed batch adds 4096 rows and rolls up to 32 rows; JMH `@OperationsPerInvocation(4096)` normalizes results per input row. Index setup and count/value validation are outside the timed batch, but GC profiled allocation includes their amortized allocations. Cases are six dense dimensions and 64 dense dimensions, with valid rows. Parsing, broker I/O, non-rollup and high-cardinality workloads are not measured.

Both sides include the retained boolean-array dimension-presence optimization. The baseline source was captured before adding this guard; it is deliberately not plain PR HEAD. Only the early return differs between measured implementations.

## Results and decision

| Workload | Baseline ns/row | Candidate ns/row | Profiled B/row before → after |
| --- | --- | --- | --- |
| 6 dense dimensions | 521.76 ± 55.09 | 480.37 ± 4.00 | 561.26 → 505.25 |
| 64 dense dimensions | 3,509.01 ± 74.13 | 3,436.84 ± 20.33 | 3,685.30 → 3,629.30 |

Retain this five-line guard as a small allocation optimization. Both cases save approximately 56 profiled bytes per input row (10.0% for six dimensions, 1.5% for 64). Timing intervals overlap in both comparisons; the lower point estimates do not establish a speedup. This decision does not depend on claiming a throughput improvement.

GPT-5.6 Luna(max) ran four measurements using Temurin JDK25/JMH1.37, one thread, two forks, three one-second warmups and four one-second measurements per fork, with the GC profiler. Each baseline/candidate pair ran consecutively. Errors shown are JMH 99.9% confidence-interval half-widths. Parent test/static-check builds finished before timing. Index totals and values validated on both sides.

## Reproduction and status

[Raw data, source fingerprints, validation logs, and runner](incremental-index-empty-error-experiments/). The archived baseline is the accepted boolean-array candidate, SHA-256 `19d0b699fc2cc0cd7691053573e914c8fbb8887d43aa3660d141401fd95da73f`. The candidate hash is `35e41fc558a3ccc6505a216659401b1faf339575196df932aae133c71f664164`. The measured harness snapshot is preserved; only final qualifiers on loop variables were added to the portable source after timing.

```sh
DRUID_BENCHMARK_FATJAR=/Users/frankchen/source/open/druid/benchmarks/target/benchmarks.jar \
  bash dev/incremental-index-empty-error-experiments/run-empty-error-benchmark.sh validate

# Use run for the four timed comparisons.
```

The retained production changes, regression tests and benchmark artifacts are included in PR #202. Full ingestion throughput remains unmeasured.
