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

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
baseline_repo="${BASELINE_REPO:-$repo_root}"
candidate_repo="${CANDIDATE_REPO:-$repo_root}"
fatjar="${DRUID_BENCHMARK_FATJAR:-}"
build_dir="${BUILD_DIR:-$script_dir/build}"
results_dir="${RESULTS_DIR:-$script_dir/build/results}"
java_cmd="${JAVA:-java}"
javac_cmd="${JAVAC:-javac}"

if [[ -z "$fatjar" ]]; then
  if [[ -f "$baseline_repo/benchmarks/target/benchmarks.jar" ]]; then
    fatjar="$baseline_repo/benchmarks/target/benchmarks.jar"
  elif [[ -f "/Users/frankchen/source/open/druid/benchmarks/target/benchmarks.jar" ]]; then
    fatjar="/Users/frankchen/source/open/druid/benchmarks/target/benchmarks.jar"
  else
    echo "Set DRUID_BENCHMARK_FATJAR to benchmarks.jar" >&2
    exit 1
  fi
fi

baseline_source="$script_dir/baseline-source/IncrementalIndex.java"
candidate_source="$candidate_repo/processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java"
benchmark_source="$script_dir/src/main/java/org/apache/druid/benchmark/DimensionPresenceBenchmark.java"

for required_path in "$fatjar" "$baseline_source" "$candidate_source" "$benchmark_source"; do
  if [[ ! -f "$required_path" ]]; then
    echo "Missing required path: $required_path" >&2
    exit 1
  fi
done

ensure_output_directories()
{
  mkdir -p "$build_dir/baseline-classes" "$build_dir/candidate-classes" "$build_dir/benchmark-classes" "$results_dir"
}

compile_sources()
{
  ensure_output_directories

  "$javac_cmd" -cp "$fatjar" \
    -d "$build_dir/baseline-classes" \
    "$baseline_source"

  "$javac_cmd" -cp "$fatjar" \
    -d "$build_dir/candidate-classes" \
    "$candidate_source"

  "$javac_cmd" -cp "$fatjar" \
    -processorpath "$fatjar" \
    -processor org.openjdk.jmh.generators.BenchmarkProcessor \
    -d "$build_dir/benchmark-classes" \
    "$benchmark_source"

  {
    echo "java: $($java_cmd -version 2>&1 | head -n 1)"
    echo "javac: $($javac_cmd -version 2>&1)"
    echo "fatjar: $fatjar"
    echo "fatjar-sha256: $(shasum -a 256 "$fatjar" | awk '{print $1}')"
    echo "baseline-repo: $baseline_repo"
    echo "baseline-source-commit: 786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6"
    echo "baseline-source-sha256: $(shasum -a 256 "$baseline_source" | awk '{print $1}')"
    echo "candidate-repo: $candidate_repo"
    echo "candidate-head: $(git -C "$candidate_repo" rev-parse HEAD)"
    echo "candidate-source-sha256: $(shasum -a 256 "$candidate_source" | awk '{print $1}')"
    echo "benchmark-source-sha256: $(shasum -a 256 "$benchmark_source" | awk '{print $1}')"
    echo "classpath: $fatjar"
  } > "$results_dir/sourcefingerprints.txt"
}

validate_variant()
{
  local variant="$1"
  "$java_cmd" -cp "$build_dir/benchmark-classes:$build_dir/$variant-classes:$fatjar" \
    org.apache.druid.benchmark.DimensionPresenceBenchmark --validate \
    > "$results_dir/validation-$variant.txt" 2>&1
}

run_variant()
{
  local variant="$1"
  local scenario="$2"
  local result_file="$results_dir/dimension-presence-$scenario-$variant.json"

  "$java_cmd" -cp "$build_dir/benchmark-classes:$build_dir/$variant-classes:$fatjar" \
    org.openjdk.jmh.Main \
    'org.apache.druid.benchmark.DimensionPresenceBenchmark.addRows' \
    -p "scenario=$scenario" \
    -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc -t 1 \
    -rf json -rff "$result_file" -foe true
}

stage="${1:-help}"
case "$stage" in
  compile)
    compile_sources
    ;;
  validate)
    compile_sources
    validate_variant baseline
    validate_variant candidate
    ;;
  run)
    compile_sources
    validate_variant baseline
    validate_variant candidate
    for scenario in sixDense sixtyFourDense sixtyFourSparse; do
      run_variant baseline "$scenario"
      run_variant candidate "$scenario"
    done
    ;;
  help|--help|-h)
    echo "Usage: $0 compile|validate|run"
    echo "Environment: DRUID_BENCHMARK_FATJAR, BASELINE_REPO, CANDIDATE_REPO, BUILD_DIR, RESULTS_DIR"
    ;;
  *)
    echo "Usage: $0 compile|validate|run" >&2
    exit 1
    ;;
esac
