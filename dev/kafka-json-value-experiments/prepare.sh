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
pr_root="${PR_ROOT:-$(git -C "$next_root" rev-parse --show-toplevel)}"
main_root="${MAIN_ROOT:-$pr_root}"
fat_jar="${FAT_JAR:-$main_root/benchmarks/target/benchmarks.jar}"
build_root="${BUILD_ROOT:-$next_root/build}"
baseline_ref="${BASELINE_REF:-786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6}"

for path in "$fat_jar" \
  "$pr_root/processing/src/main/java/org/apache/druid/io/ByteBufferInputStream.java" \
  "$pr_root/indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SettableByteEntity.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReaderUtils.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReader.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonNodeReader.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/MapInputRowParser.java" \
  "$pr_root/benchmarks/src/test/java/org/apache/druid/benchmark/JsonValueMaterializationBenchmark.java"; do
  if [[ ! -f "$path" ]]; then
    echo "Missing required file: $path" >&2
    exit 1
  fi
done

rm -rf "$build_root"
mkdir -p "$build_root/tree-classes" "$build_root/optimized-parser-classes" \
  "$build_root/baseline-parser-src" "$build_root/baseline-parser-classes" "$build_root/benchmark-classes"

# The fat benchmark jar is deliberately taken from the main checkout, where the existing Druid dependencies were
# built.  The classes below are compiled from the PR worktree and are placed first on the runtime classpath so the
# benchmark cannot accidentally use stale JsonReader or MapInputRowParser bytecode from the fat jar.
javac -cp "$fat_jar" -d "$build_root/tree-classes" \
  "$pr_root/processing/src/main/java/org/apache/druid/io/ByteBufferInputStream.java" \
  "$pr_root/indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SettableByteEntity.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReaderUtils.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonReader.java" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/JsonNodeReader.java"

javac -cp "$fat_jar" -d "$build_root/optimized-parser-classes" \
  "$pr_root/processing/src/main/java/org/apache/druid/data/input/impl/MapInputRowParser.java"

git -C "$pr_root" show "$baseline_ref:processing/src/main/java/org/apache/druid/data/input/impl/MapInputRowParser.java" \
  > "$build_root/baseline-parser-src/MapInputRowParser.java"
javac -cp "$fat_jar" -d "$build_root/baseline-parser-classes" \
  "$build_root/baseline-parser-src/MapInputRowParser.java"

javac -cp "$fat_jar:$build_root/tree-classes:$build_root/optimized-parser-classes" \
  -processorpath "$fat_jar" \
  -processor org.openjdk.jmh.generators.BenchmarkProcessor \
  -d "$build_root/benchmark-classes" \
  "$pr_root/benchmarks/src/test/java/org/apache/druid/benchmark/JsonValueMaterializationBenchmark.java"

printf 'Prepared benchmark classes: %s\n' "$build_root"
printf 'Runtime classpath: %s:%s:%s\n' \
  "$build_root/benchmark-classes" "$build_root/tree-classes:$build_root/optimized-parser-classes" "$fat_jar"
printf 'Baseline parser classes: %s\n' "$build_root/baseline-parser-classes"
