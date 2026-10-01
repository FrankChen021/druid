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

# Compare only TimestampParser implementations. KafkaInputReader is HEAD on both sides, while the PR JSON byte
# reader classes and guarded MapInputRowParser are common overrides. The bounded cases are alternating 256-byte and
# 4096-byte explicit reads plus a constant 256-byte control; no metadata-wrapper candidate is included.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="${DRUID_PR_ROOT:-$(git -C "$script_dir" rev-parse --show-toplevel)}"
dependency_root="${DRUID_DEPENDENCY_ROOT:-$(git -C "$script_dir" rev-parse --show-toplevel)}"
baseline_commit="${BASELINE_COMMIT:-786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6}"
optimized_timestamp_parser="${OPTIMIZED_TIMESTAMP_PARSER:-$repo_root/processing/src/main/java/org/apache/druid/java/util/common/parsers/TimestampParser.java}"
output_root="${OUTPUT_ROOT:-$script_dir/build/timestamp-fastpath}"

benchmark_jar="$dependency_root/benchmarks/target/benchmarks.jar"
kafka_jar="$dependency_root/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-39.0.0-SNAPSHOT.jar"
map_parser="$repo_root/processing/src/main/java/org/apache/druid/data/input/impl/MapInputRowParser.java"
benchmark_source="$repo_root/benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatReadBenchmark.java"

for required_file in "$benchmark_jar" "$kafka_jar" "$map_parser" "$optimized_timestamp_parser" "$benchmark_source"; do
  if [[ ! -f "$required_file" ]]; then
    echo "Missing required file: $required_file" >&2
    exit 1
  fi
done

rm -rf "$output_root"
mkdir -p "$output_root"/{baseline-source,optimized-source,common-classes,baseline-classes,optimized-classes,benchmark-classes}

common_classes="$output_root/common-classes"
baseline_classes="$output_root/baseline-classes"
optimized_classes="$output_root/optimized-classes"
benchmark_classes="$output_root/benchmark-classes"
deps="$benchmark_jar:$kafka_jar"

common_source_paths=(
  processing/src/main/java/org/apache/druid/io/ByteBufferInputStream.java
  processing/src/main/java/org/apache/druid/data/input/impl/JsonReaderUtils.java
  processing/src/main/java/org/apache/druid/data/input/impl/JsonReader.java
  processing/src/main/java/org/apache/druid/data/input/impl/JsonNodeReader.java
  indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SettableByteEntity.java
)
reader_path="extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java"
format_path="extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputFormat.java"
timestamp_path="processing/src/main/java/org/apache/druid/java/util/common/parsers/TimestampParser.java"
baseline_reader="$output_root/baseline-source/$reader_path"
baseline_format="$output_root/baseline-source/$format_path"
baseline_timestamp="$output_root/baseline-source/$timestamp_path"
mkdir -p "$(dirname "$baseline_reader")" "$(dirname "$baseline_format")" "$(dirname "$baseline_timestamp")"
git -C "$repo_root" show "$baseline_commit:$reader_path" > "$baseline_reader"
git -C "$repo_root" show "$baseline_commit:$format_path" > "$baseline_format"
git -C "$repo_root" show "$baseline_commit:$timestamp_path" > "$baseline_timestamp"

javac -cp "$deps" \
  -d "$common_classes" \
  "$map_parser" \
  "${common_source_paths[@]/#/$repo_root/}"

javac -cp "$deps:$common_classes" \
  -d "$baseline_classes" \
  "$baseline_format" "$baseline_reader" "$baseline_timestamp"

javac -cp "$deps:$common_classes" \
  -d "$optimized_classes" \
  "$baseline_format" "$baseline_reader" "$optimized_timestamp_parser"

javac -cp "$deps:$common_classes:$baseline_classes" \
  -processorpath "$benchmark_jar" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -d "$benchmark_classes" \
  "$benchmark_source"

if [[ "${COMPILE_ONLY:-false}" == "true" ]]; then
  printf 'Compiled classes: %s\n' "$output_root"
  exit 0
fi

run_case()
{
  local label="$1"
  local implementation_classes="$2"
  local size="$3"
  local pattern="$4"
  java -cp "$benchmark_classes:$common_classes:$implementation_classes:$deps" \
    org.openjdk.jmh.Main \
    'KafkaInputFormatReadBenchmark.parseAndRead' \
    -p messageSize="$size" -p dimensionMode=explicit -p accessMode=regular -p timestampPattern="$pattern" \
    -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
    -rf json -rff "$output_root/$label.json" -foe true
}

run_case explicit-256-alternating-baseline "$baseline_classes" 256 alternating
run_case explicit-4096-alternating-baseline "$baseline_classes" 4096 alternating
run_case explicit-256-constant-baseline "$baseline_classes" 256 constant

run_case explicit-256-alternating-optimized "$optimized_classes" 256 alternating
run_case explicit-4096-alternating-optimized "$optimized_classes" 4096 alternating
run_case explicit-256-constant-optimized "$optimized_classes" 256 constant

printf 'Results: %s\n' "$output_root"
