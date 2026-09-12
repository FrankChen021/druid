# IncrementalIndex dimension-presence benchmark

The benchmark compares the HEAD implementation with the current candidate implementation of
`IncrementalIndex.toIncrementalIndexRow` through the public `IncrementalIndex.add` path.  The candidate replaces
per-row `HashSet` bookkeeping with a dense boolean presence array.

## Results

The values below are JMH average-time scores in nanoseconds per `add` operation.  Error is the JMH 99.9% confidence
interval half-width.  Allocation is `gc.alloc.rate.norm`, normalized by `@OperationsPerInvocation(4096)`; because the
GC profiler observes the measurement interval, its allocation value includes any invocation setup/teardown work that
falls within that interval and should be read as amortized per-row allocation.

| Scenario | HEAD time (ns/add) | Candidate time (ns/add) | Time change | HEAD allocation (B/op) | Candidate allocation (B/op) | Allocation change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 6 dense dimensions | 643.971 +/- 32.577 | 497.011 +/- 56.482 | -22.8% | 873.278 +/- 0.021 | 561.252 +/- 0.072 | -35.7% |
| 64 dense dimensions | 4485.438 +/- 191.679 | 3250.597 +/- 46.788 | -27.5% | 6245.341 +/- 0.109 | 3685.300 +/- 0.006 | -41.0% |
| 64 dimensions, 32 omitted | 2984.350 +/- 215.821 | 1847.399 +/- 37.171 | -38.1% | 4732.486 +/- 49.054 | 2148.474 +/- 0.012 | -54.6% |

## Method

Rows are built once per trial as immutable `MapBasedInputRow` objects.  Each scenario adds 4,096 rows with one
timestamp and 32 repeated dimension-value groups, so the fixed-rollup index has exactly 32 entries and cannot grow
without bound.  The on-heap index is created in `@Setup(Level.Invocation)`, outside the timed method, with a maximum
row count of 64 and a `CountAggregatorFactory`.  The timed method performs 4,096 real `index.add(row)` calls and
consumes both fields of every `IncrementalIndexAddResult`.  `@OperationsPerInvocation(4096)` reports the score per add.

The six-dimension dense rows and 64-dimension dense rows contain every configured dimension.  The sparse rows contain
the 32 even-numbered dimensions and omit the 32 odd-numbered dimensions.  Invocation teardown checks the 32 rollup
rows, all 64 or 6 dimension names, every count metric (128 per rollup row), all present values, all omitted values, and
the total count of 4,096 rows.  Both variant validation logs report all three scenarios as validated.

Each variant ran as its own JMH command, paired baseline then candidate for each scenario: two forks, one thread,
three one-second warmup iterations, four one-second measurement iterations, average time mode, and the GC profiler.
The six raw result files are `dimension-presence-{scenario}-{variant}.json` in this directory.

## Reproduction

From this directory, `./run-dimension-presence-benchmark.sh compile` compiles the HEAD and candidate
`IncrementalIndex.java` sources with `javac 25` against the existing `benchmarks.jar` and runs the JMH annotation
processor.  `./run-dimension-presence-benchmark.sh validate` runs the non-timed checks.  `./run-dimension-presence-benchmark.sh run`
repeats compilation and validation before running the six timed commands sequentially.

The measured environment was JDK 25 (`25+36-LTS`) and JMH 1.37.  Source and dependency fingerprints are recorded in
`sourcefingerprints.txt`; `DimensionPresenceBenchmark.measured.java` preserves the exact benchmark source snapshot
used for timing.
