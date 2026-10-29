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

# Kafka performance PR validation

Publication update for PR #202, 2026-09-12. All retained follow-ups and their individual benchmark artifacts are included in the PR. Statements about uncommitted work in historical experiment snapshots describe their measurement-time status.

The final combined Maven run completed 236 tests: 235 passed, one skipped, no failures/errors. Processing: 184 with one skip; Protobuf: nine; Avro: 27; Kafka: 16.

```sh
mvn -o test -pl extensions-core/kafka-indexing-service,extensions-core/avro-extensions,extensions-core/protobuf-extensions -am \
  '-Dtest=org.apache.druid.segment.incremental.*Test,TimestampParserTest,TimestampSpecTest,MapInputRowParserTest,JsonReaderTest,JsonReaderBufferTest,JsonNodeReaderTest,KafkaInputFormatTest,AvroStreamReaderBufferTest,SchemaRegistryBasedAvroBytesDecoderTest,ProtobufReaderTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
```

Checkstyle passed for processing, indexing-service, Kafka, Avro, Protobuf and benchmarks. Production/test forbidden-API checks passed for all five production modules. The complete production/test diff was self-reviewed against the fetched PR target, including buffer ownership, timestamp fallback, dimension-presence semantics and deferred set publication. No new timings were collected for publication; the individual reports retain their original baselines and limitations.

Known review item: SchemaRegistryBasedAvroBytesDecoder has no explicit ThreadLocal.remove or close hook. Message bytes are cleared after decoding, but last-schema reader state can remain on long-lived worker threads. This earlier lifecycle concern remains unresolved. The PR is not claimed merge-ready on that basis.

Rejected metadata snapshot and token-materialization candidates are archived references, not enabled production changes. Full project tests, a running Druid cluster and end-to-end broker-to-index throughput were not measured.

## CI follow-up, 2026-09-14

CI on `b99da25a38` exposed three default-locale `String.format` calls in two new benchmark classes and 18 archived artifacts missing ASF license headers. The formatting calls now use Druid's locale-stable `StringUtils.format`; they construct fixtures outside the timed path. License headers were added without changing the archived payloads or benchmark measurements. The archived patch still parses, and the timestamp parity files remain identical.

The earlier forbidden-API checks covered production modules but missed the benchmark module, and Apache RAT had not been run. The corrected source export passes `mvn -o -N -Prat apache-rat:check`.

The failed unit-test shards ended with `minio/minio:latest` image downloads returning Docker HTTP 404 / pull access denied. Other reported assertion failures passed on CI's automatic retries. These external image-download failures remain unresolved; this follow-up does not claim a green full CI run.

Benchmark compilation and production/test forbidden-API checks passed for the 17-module benchmark reactor. Benchmark Checkstyle passed separately:

```sh
mvn -o test-compile forbiddenapis:check forbiddenapis:testCheck -pl benchmarks -am \
  -DskipTests -Dpmd.skip=true -Dcheckstyle.skip=true -Dweb.console.skip=true -T1C
mvn -o checkstyle:check -pl benchmarks
```

PMD was skipped after a local PMD StackOverflowError; dependency Checkstyle was separated after prolonged XPath evaluation in unchanged SQL sources. These commands validate the specific CI fixes, not the entire static-check workflow.

## Dependency analysis follow-up, 2026-09-17

CI on `e75f502b70` passed packaging, strict compilation, OpenRewrite and CodeQL, but dependency analysis found four libraries used directly by benchmarks and available only transitively. `benchmarks/pom.xml` now declares Protobuf (also required by generated compile sources), Avro, Kafka clients and the Confluent schema-registry client, using the existing shared versions.

Compilation and dependency analysis passed for the benchmark reactor with CI's strict dependency flags:

```sh
mvn -o test-compile dependency:analyze -pl benchmarks -am \
  -DoutputXML=true -DignoreNonCompile=true -DfailOnWarning=true \
  -DskipTests -Pskip-static-checks -Dweb.console.skip=true -T1C
```

All seven failed test jobs on that revision (six unit shards and Docker tests) ended in MinIO image-download failures, affecting 20 tests. This build-only correction does not resolve the external image availability problem or change performance measurements.

Maven reactor `validate` also passed with PMD and Checkstyle skipped, including dependency enforcement. A standalone module validation initially resolved incompatible locally installed snapshot POMs; validation with `-am` uses this PR's reactor and passed.
