#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Reproduce the five incremental comparisons using the existing local dependency jars.
# Usage: bash dev/run-kafka-deserialization-benchmarks.sh [1|2|3|4|5|all|compile]
set -euo pipefail

stage="${1:-all}"
case "$stage" in 1|2|3|4|5|all|compile) ;; *) echo "Expected stage 1, 2, 3, 4, 5, all, or compile" >&2; exit 1 ;; esac
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
baseline=2061c65eedb3a274217779c94747477a50e7b386
run_root="$(mktemp -d "${TMPDIR:-/tmp}/kafka-deserialization.XXXXXX")"
fat="$repo_root/benchmarks/target/benchmarks.jar"
avro="$repo_root/extensions-core/avro-extensions/target/druid-avro-extensions-39.0.0-SNAPSHOT.jar"
proto="$repo_root/extensions-core/protobuf-extensions/target/druid-protobuf-extensions-39.0.0-SNAPSHOT.jar"
kafka="$repo_root/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-39.0.0-SNAPSHOT.jar"
for jar in "$fat" "$avro" "$proto" "$kafka"; do
  if [[ ! -f "$jar" ]]; then
    echo "Build the benchmark and extension dependency jars first. Missing: $jar" >&2
    exit 1
  fi
done
deps="$fat:$avro:$proto:$kafka"
mkdir -p "$run_root"/{baseline-classes,optimized-classes,stream-classes,shared-classes,benchmark-classes}

stream_sources=(
  processing/src/main/java/org/apache/druid/io/ByteBufferInputStream.java
  processing/src/main/java/org/apache/druid/data/input/impl/JsonReader.java
  processing/src/main/java/org/apache/druid/data/input/impl/JsonNodeReader.java
  indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SettableByteEntity.java
)
changed_sources=(
  "${stream_sources[@]}"
  extensions-core/avro-extensions/src/main/java/org/apache/druid/data/input/avro/AvroStreamReader.java
  extensions-core/protobuf-extensions/src/main/java/org/apache/druid/data/input/protobuf/ProtobufReader.java
  extensions-core/avro-extensions/src/main/java/org/apache/druid/data/input/avro/SchemaRegistryBasedAvroBytesDecoder.java
  extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java
)
baseline_sources=()
for source_path in "${changed_sources[@]}"; do
  destination="$run_root/baseline-source/$source_path"
  mkdir -p "$(dirname "$destination")"
  git show "$baseline:$source_path" > "$destination"
  baseline_sources+=("$destination")
done
helper=processing/src/main/java/org/apache/druid/data/input/impl/JsonReaderUtils.java
javac -cp "$deps" -d "$run_root/baseline-classes" "${baseline_sources[@]}"
javac -cp "$deps" -d "$run_root/optimized-classes" "${changed_sources[@]}" "$helper"
javac -cp "$deps" -d "$run_root/stream-classes" "${stream_sources[@]}" "$helper"
javac -cp "$deps" -d "$run_root/shared-classes" \
  extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputFormat.java
javac -cp "$deps" -processorpath "$fat" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor -d "$run_root/benchmark-classes" \
  benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatKeyDeserializationBenchmark.java \
  benchmarks/src/test/java/org/apache/druid/benchmark/JsonInputFormatStreamingBenchmark.java \
  benchmarks/src/test/java/org/apache/druid/benchmark/BinaryInputFormatStreamingBenchmark.java \
  benchmarks/src/test/java/org/apache/druid/data/input/avro/SchemaRegistryAvroBytesDecoderBenchmark.java \
  benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatMetadataBlendingBenchmark.java

run_jmh() {
  local label="$1" extra_classes="$2" benchmark="$3"
  java -cp "$run_root/benchmark-classes:$extra_classes:$deps" \
    org.openjdk.jmh.Main "$benchmark" -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
    -rf json -rff "$run_root/$label.json" -foe true
}
if [[ "$stage" == 1 || "$stage" == all ]]; then
  run_jmh 01-key-reader-reuse "$run_root/baseline-classes" \
    'KafkaInputFormatKeyDeserializationBenchmark.normalized.*Reader'
fi
if [[ "$stage" == 2 || "$stage" == all ]]; then
  run_jmh 02-json-before "$run_root/baseline-classes" JsonInputFormatStreamingBenchmark.parseAndRead
  run_jmh 02-json-after "$run_root/optimized-classes" JsonInputFormatStreamingBenchmark.parseAndRead
fi
if [[ "$stage" == 3 || "$stage" == all ]]; then
  run_jmh 03-binary-before "$run_root/stream-classes:$run_root/baseline-classes" BinaryInputFormatStreamingBenchmark.parseAndRead
  run_jmh 03-binary-after "$run_root/stream-classes:$run_root/optimized-classes" BinaryInputFormatStreamingBenchmark.parseAndRead
fi
if [[ "$stage" == 4 || "$stage" == all ]]; then
  run_jmh 04-avro-before "$run_root/baseline-classes" SchemaRegistryAvroBytesDecoderBenchmark.parseAndRead
  run_jmh 04-avro-after "$run_root/optimized-classes" SchemaRegistryAvroBytesDecoderBenchmark.parseAndRead
fi
if [[ "$stage" == 5 || "$stage" == all ]]; then
  run_jmh 05-metadata-before "$run_root/stream-classes:$run_root/shared-classes:$run_root/baseline-classes" \
    'KafkaInputFormatMetadataBlendingBenchmark.parseAnd.*'
  run_jmh 05-metadata-after "$run_root/stream-classes:$run_root/shared-classes:$run_root/optimized-classes" \
    'KafkaInputFormatMetadataBlendingBenchmark.parseAnd.*'
fi
printf 'Results: %s\n' "$run_root"
