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

import type { QueryContextParameterMetadata } from '../druid-models';

import {
  getQueryContextCompletions,
  queryContextDescriptionToDocMarkdown,
} from './query-context-parameter-completions';

describe('query-context-parameter-completions', () => {
  describe('getQueryContextCompletions', () => {
    it('notes that the Broker may override the built-in default without CONFIG read access', () => {
      const queryContextParameters: QueryContextParameterMetadata[] = [
        {
          name: 'useCache',
          type: 'BOOLEAN',
          description: 'Whether to use the cache.',
          default_value: 'true',
          default_description: null,
          effective_value: null,
          effective_source: 'UNAUTHORIZED',
          query_languages: ['JSON', 'SQL'],
          deprecation: null,
        },
      ];

      expect(getQueryContextCompletions(false, queryContextParameters)[0].doc).toEqual({
        name: 'useCache',
        descriptionMarkdown:
          'Type: `BOOLEAN`\n\nDefault: `true` (built-in; the Broker may override it)\n\nWhether to use the cache.',
      });
    });

    it('notes a runtime property default and adds no note to a built-in default', () => {
      const parameter: QueryContextParameterMetadata = {
        name: 'useCache',
        type: 'BOOLEAN',
        description: 'Whether to use the cache.',
        default_value: 'true',
        default_description: null,
        effective_value: 'false',
        effective_source: 'RUNTIME_PROPERTY',
        query_languages: null,
        deprecation: null,
      };

      // A null query_languages does not exclude the parameter
      expect(getQueryContextCompletions(false, [parameter])[0].doc).toEqual({
        name: 'useCache',
        descriptionMarkdown:
          'Type: `BOOLEAN`\n\nDefault: `false` (runtime property; built-in: `true`)\n\nWhether to use the cache.',
      });
      expect(
        getQueryContextCompletions(false, [
          { ...parameter, effective_value: 'true', effective_source: 'BUILT_IN' },
        ])[0].doc,
      ).toEqual({
        name: 'useCache',
        descriptionMarkdown: 'Type: `BOOLEAN`\n\nDefault: `true`\n\nWhether to use the cache.',
      });
    });

    it('quotes a parameter name that needs it unless a quote is being typed', () => {
      const queryContextParameters: QueryContextParameterMetadata[] = [
        {
          name: 'my-key',
          type: 'STRING',
          description: 'A key that is not a plain identifier.',
          default_value: null,
          default_description: null,
          effective_value: null,
          effective_source: null,
          query_languages: ['SQL'],
          deprecation: null,
        },
      ];

      expect(getQueryContextCompletions(false, queryContextParameters).map(c => c.label)).toEqual([
        '"my-key"',
      ]);
      expect(getQueryContextCompletions(true, queryContextParameters).map(c => c.label)).toEqual([
        'my-key',
      ]);
    });
  });

  describe('queryContextDescriptionToDocMarkdown', () => {
    it('turns links into their text and HTML into line breaks and list items', () => {
      expect(
        queryContextDescriptionToDocMarkdown(
          'Uses [lookups](lookups.md#reverse) for `IN`.<br /><br />Options:<ul><li>`a`: first</li><li>`b`: second</li></ul>Done.',
        ),
      ).toEqual('Uses lookups for `IN`.\n\nOptions:\n- `a`: first\n- `b`: second\nDone.');
    });

    it('keeps code spans as they are and converts code tags, strong text and entities', () => {
      expect(
        queryContextDescriptionToDocMarkdown(
          '**Deprecated.** Threads are named `processing_<queryId>`; use <code>&#124;&#124;</code> &amp; more.',
        ),
      ).toEqual('*Deprecated.* Threads are named `processing_<queryId>`; use `||` & more.');
    });

    it('drops tags that removing another tag leaves behind', () => {
      expect(queryContextDescriptionToDocMarkdown('a <<b>i>tag</i> b')).toEqual('a tag b');
    });

    it('decodes entities once', () => {
      expect(
        queryContextDescriptionToDocMarkdown('Use <code>&amp;lt;</code> for &amp;lt;.'),
      ).toEqual('Use `&lt;` for &lt;.');
    });
  });
});
