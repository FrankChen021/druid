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

# Compare constant and alternating value timestamps using the same baseline class path. This is a control experiment
# for timestamp parser/cache effects; it does not compare the metadata snapshot candidate.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
dependency_root="${DRUID_DEPENDENCY_ROOT:-$(git -C "$script_dir" rev-parse --show-toplevel)}"
output_root="${OUTPUT_ROOT:-$script_dir/build/timestamp-diagnostic}"
compile_root="$output_root/compiled"
benchmark_jar="$dependency_root/benchmarks/target/benchmarks.jar"
kafka_jar="$dependency_root/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-39.0.0-SNAPSHOT.jar"

OUTPUT_ROOT="$compile_root" COMPILE_ONLY=true "$script_dir/run-kafka-input-format-read.sh"

common_classes="$compile_root/common-classes"
baseline_classes="$compile_root/baseline-classes"
benchmark_classes="$compile_root/benchmark-classes"
deps="$benchmark_jar:$kafka_jar"
mkdir -p "$output_root"

run_case()
{
  local label="$1"
  local pattern="$2"
  java -cp "$benchmark_classes:$common_classes:$baseline_classes:$deps" \
    org.openjdk.jmh.Main \
    'KafkaInputFormatReadBenchmark.parseAndRead' \
    -p messageSize=256 -p dimensionMode=explicit -p accessMode=regular -p "timestampPattern=$pattern" \
    -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
    -rf json -rff "$output_root/$label.json" -foe true
}

run_case constant constant
run_case alternating alternating

printf 'Results: %s\n' "$output_root"
