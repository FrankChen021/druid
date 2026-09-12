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
