# KafkaInputReader lazy field-set benchmark

Date: 2026-09-12

This run compares `HEAD`'s `KafkaInputReader` with the current working-tree candidate that defers construction of the blended event key set for fixed dimensions and skips `findDimensions` in that path. The current `MapInputRowParser`, `TimestampParser`, `KafkaInputFormat`, and PR JSON byte-buffer readers (`ByteBufferInputStream`, `JsonReaderUtils`, `JsonReader`, `JsonNodeReader`, and `SettableByteEntity`) are common class-path overrides for both implementations. No key or header parser is enabled.

`KafkaInputFormatReadBenchmark.parseAndRead` creates a fresh `KafkaRecordEntity` wrapper for every invocation, parses one value row, and reads the configured fields. Each setup validation and alternating pair checks the value timestamp, expected dimensions, all value fields, and Kafka timestamp/topic/partition/offset metadata. The JMH run used JMH 1.37 on Temurin JDK 25 (`25+36-LTS`), two forks, three one-second warmup iterations, four one-second measurement iterations, and the GC profiler.

| Case | Baseline ns/op | Candidate ns/op | Latency change | Baseline B/op | Candidate B/op | Allocation change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 B, explicit, regular | 1104.094 ± 46.687 | 989.184 ± 45.518 | -10.41% | 3336.008 | 2784.007 | -16.55% |
| 4096 B, explicit, regular | 5301.394 ± 58.134 | 5161.846 ± 122.353 | -2.63% | 13504.037 | 12936.036 | -4.21% |
| 256 B, explicit, metadata repeated 4x | 1257.979 ± 18.320 | 1114.137 ± 9.473 | -11.43% | 3344.009 | 2784.008 | -16.75% |
| 256 B, discovery, regular | 1819.374 ± 6.164 | 1800.877 ± 11.150 | -1.02% | 5432.013 | 5408.012 | -0.44% |

The candidate reduced measured allocation in every case. The discovery case remains close to the baseline because it still needs the combined field set for dimension discovery; the fixed-dimension cases show the larger allocation reduction. The wide regular result has overlapping run error, so its latency difference should be treated as directional.

Raw JMH JSON files for each baseline/candidate pair are in this directory. The portable runner is `/tmp/kafka-lazy-fields-benchmark/run-kafka-lazy-fields-benchmark.sh`; `COMPILE_ONLY=true` performs compilation plus the same correctness validation without timing.

The source snapshot and SHA-256 values are in `source-snapshot.txt`; the preflight output for both implementations is in `validation.log`. Baseline is commit `786c1f4cae8dc3c06830dae16ba8e1dabd0b6cf6` (snapshot SHA-256 `fae3aae69987fb7cfaa6d5ce40093f1bd13f8cdae86aa38b60e57bb9ce119f30`). Candidate snapshot SHA-256 is `ee2e924f995a35d64199e7d50fbe3aa832bd650d466e20c3b83628507e3a6ce1`.
