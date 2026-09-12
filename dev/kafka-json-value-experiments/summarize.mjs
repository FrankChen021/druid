/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import fs from 'node:fs';

const input = process.argv[2];
if (!input) {
  throw new Error('usage: node summarize.mjs results.json');
}

const results = JSON.parse(fs.readFileSync(input, 'utf8'));
console.log('dimensionMode\tmaterializer\tmessageSize\trecordShape\tns/op\t99.9% CI low\t99.9% CI high\tgc B/op');
for (const result of results) {
  const metric = result.primaryMetric;
  const confidence = metric.scoreConfidence ?? [];
  const allocation = result.secondaryMetrics?.['gc.alloc.rate.norm']?.score ?? 'n/a';
  const params = result.params;
  console.log([
    params.dimensionMode,
    params.materializer,
    params.messageSize,
    params.recordShape,
    metric.score,
    confidence[0] ?? 'n/a',
    confidence[1] ?? 'n/a',
    allocation
  ].join('\t'));
}
