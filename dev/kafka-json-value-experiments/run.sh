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

set -euo pipefail

next_root="$(cd "$(dirname "$0")" && pwd)"
main_root="${MAIN_ROOT:-$(git -C "$next_root" rev-parse --show-toplevel)}"
fat_jar="${FAT_JAR:-$main_root/benchmarks/target/benchmarks.jar}"
build_root="${BUILD_ROOT:-$next_root/build}"
result_file="${RESULT_FILE:-$next_root/results.json}"
parser_classes="${PARSER_CLASSES:-$build_root/optimized-parser-classes}"

if [[ ! -d "$build_root/benchmark-classes" || ! -d "$build_root/tree-classes" || ! -d "$parser_classes" ]]; then
  echo "Run $next_root/prepare.sh first" >&2
  exit 1
fi

java -cp "$build_root/benchmark-classes:$build_root/tree-classes:$parser_classes:$fat_jar" \
  org.openjdk.jmh.Main \
  'JsonValueMaterializationBenchmark.parseAndConsume' \
  -wi 3 -w 1s -i 4 -r 1s -f 2 -prof gc \
  -rf json -rff "$result_file" -foe true \
  "$@"

printf 'Results: %s\n' "$result_file"
