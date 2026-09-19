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

# Kafka deserialization performance investigation

Investigated on 2026-09-12. The initial investigation focused on JSON keys. Follow-up experiments apply the additional candidates sequentially; results are recorded below.

All five follow-up changes were retained. JSON key reader reuse reduced measured parsing time by 45–50%; removing the outer binary-message copy reduced it by 27–37%; schema-registry Avro state reuse reduced it by 40.6% for repeated schemas. Direct JSON array parsing improved the 256-byte case by 7.6%, and metadata blending improved ingestion reads by 7.2–13.4%. These are separate microbenchmark comparisons, not measured Kafka ingestion throughput gains. Alternating Avro schemas increased allocation by 16 B/op, and several smaller timing differences remain inconclusive.

## Original message path

1. `KafkaRecordSupplier.poll` receives `ConsumerRecord<byte[], byte[]>`. The default Kafka deserializer is `ByteArrayDeserializer`; configuring the consumer constructs the deserializer, rather than reflecting on its implementation for every message.
2. Each record is wrapped in `KafkaRecordEntity`, backed by a `ByteBuffer` around the value bytes, and an `OrderedPartitionableRecord`.
3. `SeekableStreamIndexTaskRunner` calls `StreamChunkReader.parse`. The latter sets the current entity on a reusable `SettableByteEntityReader`, reads rows, applies filtering and collects the result.
4. For plain JSON, `SettableByteEntityReader` selects non-line-splittable `JsonInputFormat`. Its default `JsonReader` is created once and reused across records, including its Jackson factory and flattener.
5. `JsonReader` opens the entity as a stream, creates a Jackson parser, reads `JsonNode` objects, applies a lazy flattening map and constructs input rows. The task subsequently adds those rows to the appenderator.
6. With `type: kafka`, `KafkaInputReader` additionally parses configured headers and keys and blends them with value fields and record metadata. Its value reader is reused, but its key reader is created for each non-null key.

## Implemented candidate: reuse normalized key format and schema

Previously, the key-reader supplier in `KafkaInputFormat.createReader` called `JsonInputFormat.withLineSplittable(keyFormat, false)` for every non-null key. For a JSON format, this creates another `JsonInputFormat`, which constructs a fresh `ObjectMapper`. Repeated messages therefore repeatedly allocate mapper infrastructure and lose its deserialization caches. The key schema was also rebuilt for each key.

The initial patch moved format normalization and key-schema construction outside the supplier. They live for the lifetime of the enclosing Kafka reader. At that stage, a fresh key reader was still created for each message. Follow-up experiment 1 below separately measures and adds JSON reader reuse while preserving per-message state for other formats.

The improvement applies to workloads using `type: kafka` with a JSON `keyFormat` and non-null keys. Plain JSON value-only ingestion does not execute this key path. Non-JSON key formats avoid repeated schema construction but do not have the same mapper allocation cost.

Validation:

```sh
mvn -o test -pl extensions-core/kafka-indexing-service -Dtest=KafkaInputFormatTest -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
mvn -o checkstyle:check -pl extensions-core/kafka-indexing-service -Dweb.console.skip=true
```

All 11 existing input-format tests pass, including mixed records with null keys, CSV keys, flattening and schema discovery. Checkstyle reports zero violations. Module-only testing used locally installed dependencies; this was not a full reactor or broker integration test.

## Benchmark results

The benchmark was run by a GPT-5.6 Luna agent with maximum reasoning, as requested. It measures the actual Druid `JsonInputFormat.createReader`, `ByteEntity`, row iterator and key-value extraction paths. The two methods reproduce the original and optimized key setup explicitly; neither includes value parsing or metadata blending. Both create a fresh key reader on every invocation.

JMH 1.37, Temurin JDK 25 (`25+36-LTS`), macOS/aarch64, one thread, two JVM forks, three one-second warmup iterations and four one-second measurement iterations per fork, with the GC profiler. Each operation parses one key. These are small fixed keys: `{"key":"sampleKey"}` and `{"key":{"nested":"sampleKey"}}`.

| Key | Original time | Optimized time | Time reduction | Original allocation | Optimized allocation |
| --- | --- | --- | --- | --- | --- |
| Simple | 1,112.2 ± 21.2 ns/op | 560.7 ± 13.5 ns/op | 49.6% (1.98× speedup) | 17,040 B/op | 8,320 B/op |
| Nested | 1,240.7 ± 42.1 ns/op | 666.3 ± 20.0 ns/op | 46.3% (1.86× speedup) | 17,680 B/op | 8,896 B/op |

The error terms are JMH's reported 99.9% confidence intervals across eight measurements. Allocations fell by 51.2% and 49.7%, respectively. Large or highly variable keys, additional flattening rules and full Kafka ingestion were not measured. A short non-forked smoke test and an earlier harness variant are excluded from these results.

Source: [`KafkaInputFormatKeyDeserializationBenchmark.java`](../benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatKeyDeserializationBenchmark.java). Full machine-readable output: [`kafka-deserialization-benchmark-results.json`](kafka-deserialization-benchmark-results.json).

Reproduction from the repository root, using the existing local benchmark fat jar:

```sh
mkdir -p /tmp/kif-jmh/classes
javac -cp benchmarks/target/benchmarks.jar \
  -processorpath benchmarks/target/benchmarks.jar \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -d /tmp/kif-jmh/classes \
  benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatKeyDeserializationBenchmark.java
java -cp /tmp/kif-jmh/classes:benchmarks/target/benchmarks.jar \
  org.openjdk.jmh.Main KafkaInputFormatKeyDeserializationBenchmark \
  -prof gc -rf json -rff /tmp/kif-jmh/results-v2.json -foe true
```

The measured dependency classes came from the pre-existing local fat jar, not a fresh full-reactor build. The benchmark source is compatible with the benchmark module without adding a Kafka extension dependency. JMH required permission to bind its local fork-communication socket.

## Candidates identified for follow-up

| Candidate | Evidence in the current source | What must be checked before changing it |
| --- | --- | --- |
| Direct JSON byte-buffer access | `JsonReader` creates a parser from `entity.open()`. `SettableByteEntity.openRaw()` allocates stream wrappers and duplicates the buffer; Jackson consumes it through its stream path. | Benchmark against direct array/offset/length parsing with a reused factory. An implementation needs a processing-layer capability that works through the streaming wrapper, respects buffer positions and slices, supports non-array buffers, and preserves `InputEntity.open()` behavior. |
| Reuse JSON key reader | After the small patch, JSON keys still create a reader, Jackson factory and flattener for every message. | Restrict or carefully specify reuse: readers such as CSV can retain per-entity header state. Test null keys, malformed keys followed by valid keys, flattening and sampling. |
| Avoid Avro and Protobuf message copies | Both `AvroStreamReader.intermediateRowIterator` and `ProtobufReader.intermediateRowIterator` call `IOUtils.toByteArray(source.open())` and then wrap the resulting array in a `ByteBuffer`. | Benchmark realistic payload sizes and allocations; define byte ownership and preserve decoder assumptions about array backing and position. |
| Reuse Avro decoder infrastructure | `SchemaRegistryBasedAvroBytesDecoder.parse` constructs `GenericDatumReader` and requests a binary decoder with no reuse object for each message. The schema-registry client already caches schemas. | Measure reader/decoder reuse with alternating schemas and account for concurrency. Reusing decoded records is unsafe if earlier input rows retain their data. |
| Reduce Kafka metadata blending allocations | `KafkaInputReader.buildBlendedRows` builds dimension sets and another map/input-row layer around the parsed value. | Profile the complete row-consumption path; preserve lazy flattening, null fallback, timestamp selection and dimension exclusions. |

The existing `JsonInputFormatBenchmark` puts 1,000 events in one entity. That is useful for batch parsing but amortizes parser setup across many events and does not represent one-event Kafka messages. Measurements for streaming should reuse reader setup across messages while opening and consuming each message separately, and should consume field values as well as returned row objects.

Parser microbenchmarks exclude Kafka fetching, decompression, transforms, indexing, persistence and publishing. Their speedups must not be presented as end-to-end ingestion throughput gains.


## Follow-up: sequential experiments

Each experiment compares against the preceding retained implementation. Timings across different benchmark workloads are not directly comparable and percentage gains must not be multiplied together. All timed runs use GPT-5.6 Luna (maximum reasoning), JMH forks and the GC profiler.

### 1. Reuse the JSON key reader

Retained. Only the built-in `JsonInputFormat` reuses its key reader through a `SettableByteEntity`; other formats and subclasses continue to receive a fresh reader per key. This preserves CSV header and custom-reader lifecycles. Default, line and node JSON modes are covered by regression tests for invalid keys, null keys, repeated valid keys, alternating read/sample calls and retention of earlier rows. All 14 Kafka input-format tests pass.

The comparator already reuses the normalized format and key schema in both variants. Each operation alternates between two keys of the same shape and consumes the parsed value.

| Key | Fresh reader (ns/op) | Reused reader (ns/op) | Time reduction | Allocation before → after |
| --- | --- | --- | --- | --- |
| Simple | 582.1 ± 19.5 | 289.4 ± 13.7 | 50.3% | 8,316 → 2,036 B/op |
| Nested | 675.5 ± 31.7 | 368.0 ± 44.9 | 45.5% | 8,868 → 2,564 B/op |

Two forks, three one-second warmups and four one-second measurements per fork; reported errors are JMH 99.9% intervals. [Raw results](kafka-deserialization-experiments/01-key-reader-reuse.json).

```sh
java -cp /tmp/kafka-perf/stage1/classes:benchmarks/target/benchmarks.jar \
  org.openjdk.jmh.Main 'KafkaInputFormatKeyDeserializationBenchmark.normalized.*Reader' \
  -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
  -rf json -rff /tmp/kafka-perf/stage1/results.json -foe true
```

The benchmark class is compiled with the same `javac`/JMH processor procedure shown above, with output directory `/tmp/kafka-perf/stage1/classes`.


### 2. Parse JSON directly from an array-backed stream

Retained. `JsonReader` and `JsonNodeReader` inspect the stream returned by `InputEntity.open()`. For the final `ByteBufferInputStream` implementation with an accessible backing array, Jackson receives the array, offset and remaining length directly. Other streams retain the normal parser path. `SettableByteEntity.openRaw()` now returns `ByteBufferInputStream` directly, removing an unnecessary delegating wrapper. Its existing public nested stream class remains available.

The implementation does not infer bytes from the entity itself, so custom `open()` implementations and decompression remain effective. Tests cover heap offsets and slices, direct/read-only buffers, buffer position/limit/mark preservation, repeated reads, sampling, Unicode and UTF-16 detection. The parser's field/features behavior is otherwise unchanged.

| Message | Before (ns/op) | After (ns/op) | Timing conclusion | Allocation before → after |
| --- | --- | --- | --- | --- |
| 256 bytes | 1,066.9 ± 23.6 | 985.8 ± 15.4 | 7.6% lower time | 3,496 → 3,408 B/op |
| 4,096 bytes | 7,673.9 ± 328.6 | 7,574.3 ± 88.2 | Intervals overlap; no conclusive timing gain | 7,336 → 7,248 B/op |

`JsonInputFormatStreamingBenchmark` reuses the normal JSON reader and sets a new `ByteEntity` for each message. It consumes every discovered field, including a nested object and a string padded to the specified size. Timestamp extraction uses a fixed missing-value default; this measures parsing/field access rather than timestamp-string parsing. One row per invocation is checked.

Identical benchmark source, two forks and three/four one-second warmup/measurement iterations were used for each classpath. Before/after `ByteBufferInputStream`, `SettableByteEntity`, `JsonReader` and `JsonNodeReader` implementations were freshly compiled from source into isolated override directories (the optimized directory also includes `JsonReaderUtils`). This avoids measuring stale production classes from the existing fat jar.

[Before results](kafka-deserialization-experiments/02-json-bytes-before.json) · [After results](kafka-deserialization-experiments/02-json-bytes-after.json)

```sh
java -cp /tmp/kafka-perf/stage2/benchmark-classes:/tmp/kafka-perf/stage1/baseline-classes:benchmarks/target/benchmarks.jar \
  org.openjdk.jmh.Main JsonInputFormatStreamingBenchmark.parseAndRead \
  -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc -rf json -rff /tmp/kafka-perf/stage2/baseline.json -foe true
java -cp /tmp/kafka-perf/stage2/benchmark-classes:/tmp/kafka-perf/stage2/optimized-classes:benchmarks/target/benchmarks.jar \
  org.openjdk.jmh.Main JsonInputFormatStreamingBenchmark.parseAndRead \
  -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc -rf json -rff /tmp/kafka-perf/stage2/optimized.json -foe true
```


### 3. Avoid the outer Avro/Protobuf message copy

Retained for the built-in inline, multiple-inline and schema-registry Avro decoders, and inline/file/schema-registry Protobuf decoders. These readers can consume a slice of the opened `ByteBufferInputStream` without allocating a full-message copy. The slice starts at zero and is bounded to the current message. Direct/read-only buffers and other streams retain the copy path so array-dependent decoders remain supported. Unknown decoder classes/subclasses and schema-repo subject converters still receive a private copy; their contracts do not prohibit input mutation.

Regression checks cover all three optimized Avro decoder variants with heap offsets, slices, direct/read-only buffers and repeated reads. Protobuf file decoding has the same buffer coverage. Both formats have tests proving a custom decoder cannot mutate source bytes. Sixty-five focused tests passed in the reactor run (14 JSON buffer, 32 Avro, 19 Protobuf).

| Format / bytes | Before (ns/op) | After (ns/op) | Time reduction | Allocation before → after |
| --- | --- | --- | --- | --- |
| Avro / 256 | 1,311.0 ± 138.3 | 851.8 ± 8.8 | 35.0% | 19,712 → 10,808 B/op |
| Avro / 4,096 | 1,563.9 ± 81.4 | 979.1 ± 29.1 | 37.4% | 31,232 → 18,488 B/op |
| Protobuf / 256 | 1,091.4 ± 22.7 | 691.4 ± 8.9 | 36.7% | 11,656 → 2,752 B/op |
| Protobuf / 4,096 | 1,363.0 ± 35.0 | 992.6 ± 23.5 | 27.2% | 23,176 → 10,432 B/op |

`BinaryInputFormatStreamingBenchmark` uses the actual inline-schema Avro and inline-descriptor Protobuf formats, with nested records, a reused reader, one message per operation, and discovered-field consumption. Generated message lengths are exactly 256 and 4,096 bytes for both formats. These results do not measure the schema-registry network or the internal copies that individual binary decoders may still make.

Two sequential classpath runs use the same harness and stage-2 stream implementation; only freshly compiled `AvroStreamReader` and `ProtobufReader` classes differ. Each has two forks and three/four one-second warmup/measurement iterations.

[Before results](kafka-deserialization-experiments/03-binary-copy-before.json) · [After results](kafka-deserialization-experiments/03-binary-copy-after.json)

The command is `org.openjdk.jmh.Main BinaryInputFormatStreamingBenchmark.parseAndRead -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc -rf json -rff RESULTS -foe true`. The classpath contains the stage-3 benchmark classes, stage-2 optimized classes, the selected stage-3 baseline/optimized classes, `benchmarks/target/benchmarks.jar`, and the Avro/Protobuf extension jars. The benchmark module adds a test-scoped Avro dependency; Protobuf was already present.


### 4. Reuse schema-registry Avro decoder infrastructure

Retained with a workload caveat. A thread-local state object retains only the last schema's `GenericDatumReader` and a reusable `BinaryDecoder`. It always reads into a new record (`read(null, ...)`) and resets the binary decoder to an empty array after each call to avoid retaining a large input message. The registry schema lookup is unchanged. Schema changes replace the reader rather than maintaining an unbounded per-schema reader cache.

Tests verify concurrent use of one decoder, schema changes, malformed-payload recovery and stability of previously returned records. Twenty-seven focused Avro tests pass.

| Schema pattern | Before (ns/op) | After (ns/op) | Timing conclusion | Allocation before → after |
| --- | --- | --- | --- | --- |
| Repeated schema | 465.9 ± 4.9 | 276.6 ± 16.8 | 40.6% lower time | 1,864 → 800 B/op |
| Alternating schemas | 467.7 ± 4.8 | 460.1 ± 7.8 | Intervals overlap; no conclusive timing gain | 1,864 → 1,880 B/op |

Alternating schemas miss the single-entry cache and increased measured allocation by 16 B/op (0.9%). The optimization is most useful when consecutive messages use the same schema. The benchmark uses 256-byte Confluent-wire messages, two structurally similar schemas, an in-memory `MockSchemaRegistryClient`, and consumes decoded record fields. It does not measure HTTP requests or full ingestion.

The same `SchemaRegistryAvroBytesDecoderBenchmark` was run with freshly compiled baseline/optimized decoder classes, two forks and three/four one-second warmup/measurement iterations. [Before results](kafka-deserialization-experiments/04-avro-state-before.json) · [After results](kafka-deserialization-experiments/04-avro-state-after.json).

The JMH invocation is `SchemaRegistryAvroBytesDecoderBenchmark.parseAndRead -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc -rf json -rff RESULTS -foe true`; classpaths include the stage-4 benchmark classes, selected stage-4 decoder override, benchmark fat jar and Avro extension jar.


### 5. Reduce Kafka metadata dimension-set copies

Retained. `KafkaInputReader` builds the blended event's dimension set once and uses a set-difference view to exclude the internal timestamp column from row dimensions. It preserves lazy field access, null-value fallback to Kafka metadata, the raw internal timestamp header, and value-first dimension insertion order. Regression coverage includes colliding Java string hashes (`Aa` and `BB`), explicit dimensions, discovery, reads and sampling. The final JSON/Kafka run passed 28 JSON tests and 15 Kafka input-format tests.

| Operation / dimensions | Before (ns/op) | After (ns/op) | Timing conclusion | Allocation before → after |
| --- | --- | --- | --- | --- |
| Read / discovery | 1,851.0 ± 11.5 | 1,717.9 ± 37.3 | 7.2% lower time | 6,088 → 5,528 B/op |
| Read / explicit | 1,487.7 ± 25.0 | 1,288.3 ± 30.3 | 13.4% lower time | 4,528 → 4,000 B/op |
| Sample / discovery | 3,603.4 ± 76.8 | 3,524.6 ± 51.9 | Intervals overlap; no conclusive timing gain | 10,160 → 9,652 B/op |
| Sample / explicit | 3,276.4 ± 67.4 | 3,063.1 ± 54.6 | 6.5% lower time | 8,128 → 7,624 B/op |

`KafkaInputFormatMetadataBlendingBenchmark` uses the real Kafka input format with a 256-byte JSON value, a reused reader, timestamp-string parsing and complete field consumption. Key and header parsers are disabled. A prebuilt consumer record is wrapped in a new Kafka entity per operation. Discovery produces 11 dimensions; the explicit schema selects six. Each invocation verifies one row or raw sample.

Both variants retain earlier stream improvements and use the same input-format setup; only `KafkaInputReader` differs. Two forks and three/four one-second warmup/measurement iterations were used. [Before results](kafka-deserialization-experiments/05-metadata-before.json) · [After results](kafka-deserialization-experiments/05-metadata-after.json).

### Reproduction and validation

All follow-up measurements used Temurin JDK 25 (`25+36-LTS`), JMH 1.37, one benchmark thread, two forks, three one-second warmup iterations and four one-second measurement iterations per fork, with the GC profiler. Reported errors are JMH 99.9% confidence-interval half-widths. Timed runs were sequential and did not overlap the regression test builds.

The [reproduction script](run-kafka-deserialization-benchmarks.sh) compiles production overrides and the benchmark harnesses into isolated temporary directories. Baseline sources come from commit `2061c65eedb3a274217779c94747477a50e7b386`; the appropriate earlier changes are included in each comparison. It requires the existing benchmark fat jar and versioned Avro, Protobuf and Kafka extension jars listed in the script. This is a local reproduction helper for this checkout and its dependency versions. It does not require a running Kafka broker or schema-registry server.

```sh
# Compile all variants without running measurements.
bash dev/run-kafka-deserialization-benchmarks.sh compile

# Run an individual comparison, or all five sequentially.
bash dev/run-kafka-deserialization-benchmarks.sh 5
bash dev/run-kafka-deserialization-benchmarks.sh all
```

The script's syntax and compile-only mode passed. Focused JSON, Kafka, Avro and Protobuf regression runs passed, including a final rerun of all 14 schema-registry Avro decoder tests after adopting the repository's executor helper. Checkstyle passed for processing, indexing-service, Kafka, Avro, Protobuf and benchmarks. Forbidden-API checks passed for production and test classes in all five affected production modules. `git diff --check` passed. The full project suite and broker-to-index throughput were not run.
