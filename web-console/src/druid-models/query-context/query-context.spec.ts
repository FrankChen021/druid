/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { QueryContextParameterMetadata } from './query-context';
import { isSqlQueryContextParameter } from './query-context';

describe('query-context', () => {
  it('keeps the parameters that a SET statement can provide', () => {
    const parameter: QueryContextParameterMetadata = {
      name: 'useCache',
      type: 'BOOLEAN',
      description: 'Whether to use the cache.',
      default_value: 'true',
      default_description: null,
      effective_value: null,
      effective_source: null,
      query_languages: ['JSON', 'SQL'],
      deprecation: null,
    };

    expect(isSqlQueryContextParameter(parameter)).toBe(true);
    expect(isSqlQueryContextParameter({ ...parameter, query_languages: null })).toBe(true);
    // Only native (JSON) queries can provide it
    expect(isSqlQueryContextParameter({ ...parameter, query_languages: ['JSON'] })).toBe(false);
  });
});
