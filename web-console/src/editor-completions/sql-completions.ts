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

import type { CompletionContext } from '@codemirror/autocomplete';
import type { EditorState } from '@codemirror/state';
import type { SyntaxNode } from '@lezer/common';
import { C, filterMap, N, RefName, T } from 'druid-query-toolkit';

import type { CompletionWord } from '../components/code-editor/completion-source';
import { completeSyntaxTree, tokenBefore } from '../components/code-editor/completion-source';
import type { QueryContextParameterMetadata } from '../druid-models';
import { DEFAULT_SERVER_QUERY_CONTEXT } from '../druid-models';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../editor-languages/dsql-docs';
import { SQL_CONSTANTS, SQL_DYNAMICS, SQL_KEYWORDS } from '../editor-languages/dsql-keywords';
import type { AvailableFunctions } from '../helpers';
import type { ColumnMetadata } from '../utils';
import { lookupBy, uniq } from '../utils';

import type { EditorCompletion } from './editor-completion';

const SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_FUNCTION = [
  'AS',
  'ASC',
  'DESC',
  'LIMIT',
  'OFFSET',
  'RETURNING',
  'SET',
  'VALUES',
  'FETCH',
  'FIRST',
  'NEXT',
  'ONLY',
  'PRECEDING',
  'FOLLOWING',
  'UNBOUNDED',
];

const SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_REF = [
  'ASC',
  'DESC',
  'LIMIT',
  'OFFSET',
  'FIRST',
  'NEXT',
  'ONLY',
  'PRECEDING',
  'FOLLOWING',
  'UNBOUNDED',
  'CUBE',
  'ROLLUP',
  'FOR',
  'OUTER',
  'CROSS',
  'INNER',
  'LEFT',
  'RIGHT',
  'FULL',
  'NATURAL',
  'UNION',
  'INTERSECT',
  'EXCEPT',
];

const SQL_KEYWORD_FOLLOW_SUGGESTIONS: Record<string, string[]> = {
  // Keywords that can be followed by specific keywords
  SELECT: ['DISTINCT', 'ALL'],
  GROUP: ['BY'],
  ORDER: ['BY'],
  PARTITION: ['BY'],
  PARTITIONED: ['BY'],
  CLUSTERED: ['BY'],
  UNION: ['ALL'],
  INSERT: ['INTO'],
  REPLACE: ['INTO'],
  MERGE: ['INTO'],
  UPDATE: ['SET'],
  LEFT: ['JOIN', 'OUTER'],
  RIGHT: ['JOIN', 'OUTER'],
  INNER: ['JOIN'],
  FULL: ['JOIN', 'OUTER'],
  CROSS: ['JOIN'],
  NATURAL: ['JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL'],
  EXPLAIN: ['PLAN'],
  PLAN: ['FOR'],
  GROUPING: ['SETS'],
  FETCH: ['FIRST', 'NEXT'],
  FIRST: ['ROW', 'ROWS'],
  NEXT: ['ROW', 'ROWS'],
  ROWS: ['ONLY'],
  ROW: ['ONLY'],
  IS: ['NOT', 'DISTINCT'],
  DISTINCT: ['FROM'],
  BY: ['GROUPING', 'ROLLUP', 'CUBE', 'ALL', 'HOUR', 'DAY', 'MONTH', 'YEAR'],
  ALL: ['TIME'],

  // Keywords that cannot be followed by any other keyword
  AS: [],
  ASC: [],
  DESC: [],
  LIMIT: [],
  OFFSET: [],
  RETURNING: [],
  SET: [],
  VALUES: [],
  ONLY: [],
  WHERE: [],
  HAVING: [],
  USING: [],
  ON: [],
  CUBE: [],
  ROLLUP: [],
  OVERWRITE: [],
  PIVOT: [],
  UNPIVOT: [],
  MATCHED: [],
  PRECEDING: [],
  FOLLOWING: [],
  UNBOUNDED: [],
  CURRENT: [],
  EXTEND: [],
  WINDOW: [],
  RANGE: [],
  KEY: [],
  VALUE: [],
  TIME: [],
  EPOCH: [],
  MILLISECOND: [],
  SECOND: [],
  MINUTE: [],
  HOUR: [],
  DAY: [],
  DOW: [],
  ISODOW: [],
  DOY: [],
  WEEK: [],
  MONTH: [],
  QUARTER: [],
  YEAR: [],
  ISOYEAR: [],
  DECADE: [],
  CENTURY: [],
  MILLENNIUM: [],
};

const COMMENT_NODES = new Set(['LineComment', 'BlockComment', 'Issue']);

// The tokens that are words, the specializer in dsql-tokens.ts tells them apart
const WORD_NODES = new Set(['Keyword', 'FunctionName', 'Constant', 'TypeName', 'Identifier']);

/**
 * Whether a string literal or quoted identifier (the text of its token) is closed, an unterminated one lasts until the
 * end of the line
 */
function isClosed(tokenText: string): boolean {
  return tokenText.length >= 2 && tokenText.endsWith(tokenText[0]);
}

/**
 * The last token (that is not a comment) that ends before the given position, it can be on an earlier line
 */
function tokenEndingBefore(state: EditorState, pos: number): SyntaxNode | undefined {
  const cursor = completeSyntaxTree(state).cursorAt(pos, -1);
  do {
    const { node } = cursor;
    if (
      node.to <= pos &&
      node.from < node.to &&
      !node.firstChild &&
      !node.type.isTop &&
      !COMMENT_NODES.has(node.name)
    ) {
      return node;
    }
  } while (cursor.prev());
  return;
}

export interface SqlCompletionOptions {
  columnMetadata?: readonly ColumnMetadata[];
  /** The query context parameters to suggest after SET, from sys.query_context_parameters */
  queryContextParameters?: readonly QueryContextParameterMetadata[];
  columns?: readonly string[];
  availableSqlFunctions?: AvailableFunctions;
  skipAggregates?: boolean;
}

/**
 * The completions for the word being typed in DruidSQL. Needs the dsql language for the syntax tree.
 */
export function getSqlCompletions(
  { state, pos }: CompletionContext,
  { from, text: prefix }: CompletionWord,
  {
    columnMetadata,
    columns,
    availableSqlFunctions,
    skipAggregates,
    queryContextParameters,
  }: SqlCompletionOptions = {},
): EditorCompletion[] {
  const token = tokenBefore(state, pos);

  // We are in a comment
  if (COMMENT_NODES.has(token.name)) {
    return [];
  }

  // If we are autocompleting inside a literal, then don't do any of the standard suggestions.
  // Only autocomplete other literals. The imagined use-case for this is if you have `country = 'France'` or `TIMESTAMP '2024-03-02 O1:00:00'` you might want to reuse the literals
  // Right after a closed literal is not inside it
  if (
    token.name === 'String' &&
    (pos < token.to || !isClosed(state.sliceDoc(token.from, token.to)))
  ) {
    return getSqlLiterals(state, 100, prefix).map(label => ({
      label,
      boost: 1,
      detail: 'local',
    }));
  }

  // The word before the word being typed (before its quote if it is quoted), it can be on an earlier line
  const quote = state.sliceDoc(from - 1, from) === '"';
  const tokenBeforePrefix = tokenEndingBefore(state, quote ? from - 1 : from);
  const keywordBeforePrefix =
    tokenBeforePrefix && WORD_NODES.has(tokenBeforePrefix.name)
      ? state.sliceDoc(tokenBeforePrefix.from, tokenBeforePrefix.to).toUpperCase()
      : undefined;

  // Other than literals, do not autocomplete numbers
  if (/^\d+$/.test(prefix)) {
    return []; // Don't start completing if the user is typing a number
  }

  // After SET only a query context parameter name can follow
  if (keywordBeforePrefix === 'SET') {
    return getQueryContextCompletions(quote, queryContextParameters);
  }

  const possibleReferences = getPossibleSqlReferences(state, 100, prefix);

  let completions: EditorCompletion[] = possibleReferences.map(label => ({
    label,
    boost: 1,
    detail: 'local',
  }));

  if (!quote) {
    completions = completions.concat(
      (SQL_KEYWORD_FOLLOW_SUGGESTIONS[keywordBeforePrefix || ''] || SQL_KEYWORDS).map(v => ({
        label: v,
        boost: 10,
        detail: 'keyword',
      })),
      SQL_CONSTANTS.map(v => ({ label: v, boost: 11, detail: 'constant' })),
      Array.from(SQL_DATA_TYPES.entries()).map(([name, [runtime, description]]) => {
        return {
          label: name,
          boost: 31,
          detail: 'type',
          doc: {
            name,
            syntax: `Druid runtime type: ${runtime}`,
            descriptionMarkdown: description,
          },
        };
      }),
    );

    if (
      !keywordBeforePrefix ||
      !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_FUNCTION.includes(keywordBeforePrefix)
    ) {
      completions = completions.concat(
        SQL_DYNAMICS.map(v => ({ label: v, boost: 20, detail: 'dynamic' })),
      );

      // If availableSqlFunctions map is provided, use it; otherwise fall back to static SQL_FUNCTIONS
      if (availableSqlFunctions) {
        completions = completions.concat(
          filterMap(Array.from(availableSqlFunctions.entries()), ([name, funcDef]) => {
            if (skipAggregates && funcDef.isAggregate) return;
            const description = SQL_FUNCTIONS.get(name)?.[1];
            return {
              label: name,
              boost: 30,
              detail: funcDef.isAggregate ? 'aggregate' : 'function',
              doc: {
                name,
                // A function with several signatures shows them all, one per line
                syntax: funcDef.args.map(args => `${name}(${args})`).join('\n'),
                descriptionMarkdown: description,
              },
            };
          }),
        );
      } else {
        completions = completions.concat(
          Array.from(SQL_FUNCTIONS.entries()).map(([name, argDesc]) => {
            const [args, description] = argDesc;
            return {
              label: name,
              boost: 30,
              detail: 'function',
              doc: { name, syntax: `${name}(${args})`, descriptionMarkdown: description },
            };
          }),
        );
      }
    }
  }

  if (
    !keywordBeforePrefix ||
    !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_REF.includes(keywordBeforePrefix)
  ) {
    if (columnMetadata?.length) {
      const possibleReferencesLookup = lookupBy(possibleReferences, String, () => true);

      completions = completions.concat(
        uniq(columnMetadata.map(({ TABLE_SCHEMA }) => TABLE_SCHEMA)).map(schema => ({
          label: quote ? schema : String(N(schema)),
          boost: 30,
          detail: 'schema',
        })),
        uniq(
          filterMap(columnMetadata, ({ TABLE_SCHEMA, TABLE_NAME }) =>
            TABLE_SCHEMA === 'druid' || possibleReferencesLookup[TABLE_SCHEMA]
              ? TABLE_NAME
              : undefined,
          ),
        ).map(table => ({
          label: quote ? table : String(T(table)),
          boost: 40,
          detail: 'table',
        })),
        uniq(
          filterMap(columnMetadata, d =>
            possibleReferencesLookup[d.TABLE_NAME] ? d.COLUMN_NAME : undefined,
          ),
        ).map(v => ({
          label: quote ? v : String(C(v)),
          boost: 50,
          detail: 'column',
        })),
      );
    }

    if (columns?.length) {
      completions = completions.concat(
        columns.map(column => ({
          label: quote ? column : String(C(column)),
          boost: 50,
          detail: 'column',
        })),
      );
    }
  }

  return completions;
}

function isSqlParameter(parameter: QueryContextParameterMetadata): boolean {
  return !parameter.query_languages || parameter.query_languages.includes('SQL');
}

/**
 * The query context parameter names to suggest after SET: the parameters loaded from sys.query_context_parameters, or
 * the well-known context keys when they are not loaded
 */
function getQueryContextCompletions(
  quote: boolean,
  queryContextParameters: readonly QueryContextParameterMetadata[] | undefined,
): EditorCompletion[] {
  if (!queryContextParameters?.some(isSqlParameter)) {
    return Object.keys(DEFAULT_SERVER_QUERY_CONTEXT).map(key => ({
      label: quote ? key : contextKeyLabel(key),
      boost: 50,
      detail: 'context',
    }));
  }

  // Parameters that only native (JSON) queries can provide are not offered for SET
  return queryContextParameters.filter(isSqlParameter).map(parameter => ({
    label: quote ? parameter.name : contextKeyLabel(parameter.name),
    // Deprecated parameters sort after the others
    boost: parameter.deprecation ? 40 : 50,
    detail: 'context',
    doc: {
      name: parameter.name,
      descriptionMarkdown: queryContextParameterDocMarkdown(parameter),
    },
  }));
}

const HTML_ENTITIES: Record<string, string> = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
};

function decodeHtmlEntities(text: string): string {
  return text.replace(/&(#\d+|[a-z]+);/gi, (entity, code: string) => {
    if (code.startsWith('#')) return String.fromCodePoint(Number(code.slice(1)));
    return HTML_ENTITIES[code.toLowerCase()] ?? entity;
  });
}

/**
 * Converts text outside of code spans: links become their text, <br /> becomes a line break, list items become "- "
 * lines, **strong** becomes *emphasis*, other HTML tags are dropped and HTML entities are decoded
 */
function convertHtmlText(text: string): string {
  return decodeHtmlEntities(
    text
      .replace(/\[([^[\]]*)\]\([^()]*\)/g, '$1')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<li>/gi, '\n- ')
      .replace(/<\/(ul|ol)>/gi, '\n')
      .replace(/<\/?[a-z][^>]*>/gi, '')
      .replace(/\*\*([^*]+)\*\*/g, '*$1*'),
  );
}

/**
 * Converts text outside of code spans, where <code> becomes a code span. Each part is decoded once: the content of a
 * <code> tag on its own, the text around it by convertHtmlText.
 */
function convertDescriptionText(text: string): string {
  return text
    .split(/<code>([^<]*)<\/code>/i)
    .map((part, i) => (i % 2 ? `\`${decodeHtmlEntities(part)}\`` : convertHtmlText(part)))
    .join('');
}

/**
 * Converts the Markdown of a query context parameter description, which comes from the Druid docs, into the "doc
 * markdown" that completion docs render. Code spans are kept as they are (angle brackets in them are text, not HTML).
 * A paragraph break (<br /><br />) stays as an empty line.
 */
export function queryContextDescriptionToDocMarkdown(description: string): string {
  return description
    .split(/(`[^`]*`)/)
    .map((part, i) => (i % 2 ? part : convertDescriptionText(part)))
    .join('')
    .split('\n')
    .map(line => line.trim())
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * The name of a query context parameter after SET: as is when it can be written unquoted (SET keys are case-sensitive
 * as typed, like `SET sqlTimeZone = 'UTC'`), quoted otherwise, such as for a reserved keyword
 */
function contextKeyLabel(key: string): string {
  return RefName.needsQuotes(key) ? String(C(key)) : key;
}

function docCode(value: string): string {
  return `\`${value.replace(/`/g, '')}\``;
}

const EFFECTIVE_SOURCE_LABELS: Record<string, string> = {
  RUNTIME_PROPERTY: 'runtime property',
  BROKER_DYNAMIC_CONFIG: 'Broker dynamic config',
};

/**
 * Where the default comes from when it is not the built-in one, like " (Broker dynamic config; built-in: `false`)"
 */
function effectiveSourceNote(parameter: QueryContextParameterMetadata): string {
  const label = parameter.effective_source
    ? EFFECTIVE_SOURCE_LABELS[parameter.effective_source]
    : undefined;
  if (!label) return '';
  return parameter.default_value != null
    ? ` (${label}; built-in: ${docCode(parameter.default_value)})`
    : ` (${label})`;
}

/**
 * The doc of a query context parameter: its type, its default and then its description, separated by empty lines
 */
function queryContextParameterDocMarkdown(parameter: QueryContextParameterMetadata): string {
  const sections = [`Type: \`${parameter.type}\``];
  if (parameter.effective_value != null) {
    sections.push(
      `Default: ${docCode(parameter.effective_value)}${effectiveSourceNote(parameter)}`,
    );
  } else if (parameter.default_value != null) {
    // Without CONFIG read access the Broker's effective default is hidden, so the built-in one may be overridden
    sections.push(
      `Default: ${docCode(parameter.default_value)}${
        parameter.effective_source === 'UNAUTHORIZED'
          ? ' (built-in; the Broker may override it)'
          : ''
      }`,
    );
  } else if (parameter.default_description) {
    sections.push(
      `Default: ${queryContextDescriptionToDocMarkdown(parameter.default_description)}`,
    );
  }
  sections.push(queryContextDescriptionToDocMarkdown(parameter.description));
  if (parameter.deprecation) {
    sections.push(`*Deprecated*: ${queryContextDescriptionToDocMarkdown(parameter.deprecation)}`);
  }
  return sections.join('\n\n');
}

/**
 * The (closed) string literals in the text. The prefix (the word being typed) is left out since it shows up as a
 * literal itself. Needs the dsql language for the syntax tree.
 */
export function getSqlLiterals(state: EditorState, maxWords: number, prefix?: string): string[] {
  const literals: string[] = [];
  completeSyntaxTree(state).iterate({
    enter: ({ name, from, to }) => {
      if (name !== 'String') return;
      const text = state.sliceDoc(from, to);
      if (isClosed(text) && text.length >= 4) literals.push(text.slice(1, -1));
    },
  });

  return uniq(literals.filter(m => m !== prefix)).slice(0, maxWords);
}

/**
 * The identifiers in the text that could be references (quoted ones first), at least two characters long. The prefix
 * (the word being typed) is left out since it shows up as a reference itself. Needs the dsql language for the syntax
 * tree.
 */
export function getPossibleSqlReferences(
  state: EditorState,
  maxWords: number,
  prefix?: string,
): string[] {
  const quoted: string[] = [];
  const naked: string[] = [];
  completeSyntaxTree(state).iterate({
    enter: ({ name, from, to }) => {
      const text = state.sliceDoc(from, to);
      if (name === 'QuotedIdentifier') {
        if (isClosed(text) && /^"\w{2,}"$/.test(text)) quoted.push(text.slice(1, -1));
      } else if (name === 'Identifier') {
        // Keywords, functions, constants and types are not identifiers
        if (/^[a-zA-Z]\w+$/.test(text)) naked.push(text);
      }
    },
  });

  return uniq([...quoted, ...naked].filter(v => v !== prefix)).slice(0, maxWords);
}
