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

# Compare only KafkaInputReader implementations. The current PR MapInputRowParser, TimestampParser, KafkaInputFormat,
# and JSON byte-buffer reader classes are compiled into the common class path for both sides. Each side then supplies
# either HEAD's KafkaInputReader or the working-tree candidate. The bounded run below uses four cases, so the complete
# comparison is eight JMH invocations (four cases for each implementation).
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="${DRUID_PR_ROOT:-$(git -C "$script_dir" rev-parse --show-toplevel)}"
dependency_root="${DRUID_DEPENDENCY_ROOT:-$repo_root}"
baseline_commit="${BASELINE_COMMIT:-786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6}"
optimized_reader="${OPTIMIZED_READER:-$repo_root/extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java}"
output_root="${OUTPUT_ROOT:-$script_dir/build}"

benchmark_jar="$dependency_root/benchmarks/target/benchmarks.jar"
kafka_jar="$dependency_root/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-39.0.0-SNAPSHOT.jar"
map_parser="$repo_root/processing/src/main/java/org/apache/druid/data/input/impl/MapInputRowParser.java"
timestamp_parser="$repo_root/processing/src/main/java/org/apache/druid/java/util/common/parsers/TimestampParser.java"
kafka_format="$repo_root/extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputFormat.java"
benchmark_source="${BENCHMARK_SOURCE:-$repo_root/benchmarks/src/test/java/org/apache/druid/benchmark/KafkaInputFormatReadBenchmark.java}"
validation_source="$script_dir/src/org/apache/druid/benchmark/KafkaInputFormatReadValidation.java"

for required_file in \
  "$benchmark_jar" \
  "$kafka_jar" \
  "$map_parser" \
  "$timestamp_parser" \
  "$kafka_format" \
  "$optimized_reader" \
  "$benchmark_source" \
  "$validation_source"; do
  if [[ ! -f "$required_file" ]]; then
    echo "Missing required file: $required_file" >&2
    exit 1
  fi
done

rm -rf "$output_root"
mkdir -p "$output_root"/{baseline-source,candidate-source,common-classes,baseline-classes,candidate-classes,benchmark-classes,validation-classes}

common_classes="$output_root/common-classes"
baseline_classes="$output_root/baseline-classes"
candidate_classes="$output_root/candidate-classes"
benchmark_classes="$output_root/benchmark-classes"
validation_classes="$output_root/validation-classes"
deps="$benchmark_jar:$kafka_jar"

common_source_paths=(
  "$repo_root/processing/src/main/java/org/apache/druid/io/ByteBufferInputStream.java"
  "$repo_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReaderUtils.java"
  "$repo_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReader.java"
  "$repo_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonNodeReader.java"
  "$repo_root/indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SettableByteEntity.java"
  "$timestamp_parser"
  "$kafka_format"
)

reader_path="extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java"
baseline_reader="$output_root/baseline-source/$reader_path"
candidate_reader="$output_root/candidate-source/$reader_path"
mkdir -p "$(dirname "$baseline_reader")"
mkdir -p "$(dirname "$candidate_reader")"
git -C "$repo_root" show "$baseline_commit:$reader_path" > "$baseline_reader"
cp "$optimized_reader" "$candidate_reader"
{
  printf 'baseline_commit=%s\n' "$(git -C "$repo_root" rev-parse "$baseline_commit")"
  printf 'candidate_source=%s\n' "$optimized_reader"
  shasum -a 256 "$baseline_reader" "$candidate_reader"
} > "$output_root/source-snapshot.txt"

# Compile the current PR parser, timestamp fast path, KafkaInputFormat, and direct-array JSON readers once. Keeping
# these classes common ensures this comparison measures the KafkaInputReader field-set change only.
javac -cp "$common_classes:$deps" \
  -d "$common_classes" \
  "$map_parser" \
  "${common_source_paths[@]}"

javac -cp "$common_classes:$deps" \
  -d "$baseline_classes" \
  "$baseline_reader"

javac -cp "$common_classes:$deps" \
  -d "$candidate_classes" \
  "$optimized_reader"

javac -cp "$common_classes:$deps" \
  -processorpath "$benchmark_jar" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -d "$benchmark_classes" \
  "$benchmark_source"

javac -cp "$benchmark_classes:$common_classes:$deps" \
  -d "$validation_classes" \
  "$validation_source"

validate_impl()
{
  local label="$1"
  local implementation_classes="$2"
  java -cp "$validation_classes:$benchmark_classes:$common_classes:$implementation_classes:$deps" \
    org.apache.druid.benchmark.KafkaInputFormatReadValidation "$label" 2>&1 | tee -a "$output_root/validation.log"
}

# Run setup and one alternating pair for every requested case before starting any timed JMH work. The validation class
# checks timestamps, dimensions, value fields, and Kafka metadata (including repeated metadata reads).
validate_impl baseline "$baseline_classes"
validate_impl candidate "$candidate_classes"

if [[ "${COMPILE_ONLY:-false}" == "true" ]]; then
  printf 'Compiled and validated classes: %s\n' "$output_root"
  exit 0
fi

run_jmh()
{
  local label="$1"
  local implementation_classes="$2"
  local size="$3"
  local dimensions="$4"
  local access="$5"
  java -cp "$benchmark_classes:$common_classes:$implementation_classes:$deps" \
    org.openjdk.jmh.Main \
    'KafkaInputFormatReadBenchmark.parseAndRead' \
    -p "messageSize=$size" -p "dimensionMode=$dimensions" -p "accessMode=$access" -p timestampPattern=alternating \
    -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
    -rf json -rff "$output_root/$label.json" -foe true
}

run_jmh explicit-256-alternating-regular-baseline "$baseline_classes" 256 explicit regular
run_jmh explicit-256-alternating-regular-candidate "$candidate_classes" 256 explicit regular

run_jmh explicit-4096-alternating-regular-baseline "$baseline_classes" 4096 explicit regular
run_jmh explicit-4096-alternating-regular-candidate "$candidate_classes" 4096 explicit regular

run_jmh explicit-256-alternating-metadata-repeated-baseline "$baseline_classes" 256 explicit metadataRepeated
run_jmh explicit-256-alternating-metadata-repeated-candidate "$candidate_classes" 256 explicit metadataRepeated

run_jmh discovery-256-alternating-regular-baseline "$baseline_classes" 256 discovery regular
run_jmh discovery-256-alternating-regular-candidate "$candidate_classes" 256 discovery regular

printf 'Results: %s\n' "$output_root"
