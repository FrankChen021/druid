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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Downloads and validates a complete response before any rows can enter an ingestion pipeline. */
class RemoteDruidInputSourceClient implements AutoCloseable
{
  static final int MAX_ROW_BYTES = 1024 * 1024;
  static final int MAX_ROW_TOKENS = 100_000;
  static final int MAX_METADATA_BYTES = 1024 * 1024;

  private final RemoteDruidConnection connection;
  private final ObjectMapper mapper;
  private final RemoteDruidHttpClient httpClient;

  RemoteDruidInputSourceClient(final RemoteDruidConnection connection, final ObjectMapper mapper)
  {
    this.connection = connection;
    this.mapper = mapper;
    this.httpClient = connection.getAuthentication().createClient(connection.getConnectTimeout(), connection.getReadTimeout());
  }

  @Override
  public void close()
  {
    httpClient.close();
  }

  @Nullable
  Interval timeBoundary(final String dataSource) throws IOException
  {
    final File response = download(
        Map.of("queryType", "timeBoundary", "dataSource", dataSource), null,
        Math.min(connection.getMaxResponseBytes(), 1024 * 1024)
    );
    try {
      final JsonNode result = mapper.readTree(response);
      if (result.isArray() && result.isEmpty()) {
        return null;
      }
      if (!result.isArray() || result.size() != 1 || !result.get(0).path("result").hasNonNull("minTime")
          || !result.get(0).path("result").hasNonNull("maxTime")) {
        throw new IOException("Invalid remote Druid time boundary response");
      }
      try {
        final JsonNode bounds = result.get(0).get("result");
        final long start = timestamp(bounds.get("minTime"));
        final long end = Math.addExact(timestamp(bounds.get("maxTime")), 1);
        return Intervals.utc(start, end);
      }
      catch (RuntimeException e) {
        throw new IOException("Invalid remote Druid time boundary timestamps");
      }
    }
    finally {
      Files.deleteIfExists(response.toPath());
    }
  }

  RowSignature discoverSchema(final String dataSource) throws IOException
  {
    final File response = download(Map.of(
        "queryType", "segmentMetadata", "dataSource", dataSource,
        "intervals", List.of(Intervals.ETERNITY.toString()), "merge", true, "analysisTypes", List.of()
    ), null, Math.min(connection.getMaxResponseBytes(), MAX_METADATA_BYTES));
    try {
      final JsonNode result = mapper.readTree(response);
      require(result.isArray() && result.size() == 1 && result.get(0).path("columns").isObject());
      final JsonNode columns = result.get(0).get("columns");
      final RowSignature.Builder signature = RowSignature.builder();
      final List<String> names = new ArrayList<>();
      columns.fieldNames().forEachRemaining(names::add);
      names.sort(Comparator.comparing((String name) -> !"__time".equals(name)).thenComparing(name -> name));
      for (final String name : names) {
        final JsonNode column = columns.get(name);
        if (column.hasNonNull("errorMessage") && !column.get("errorMessage").asText().isEmpty()) {
          throw new IOException("Remote Druid column metadata is ambiguous; provide an explicit EXTEND schema");
        }
        final JsonNode typeNode = column.hasNonNull("typeSignature") ? column.get("typeSignature") : column.get("type");
        require(typeNode != null && typeNode.isTextual());
        final ColumnType type;
        try {
          type = ColumnType.fromString(typeNode.textValue());
        }
        catch (RuntimeException e) {
          throw new IOException("Unsupported remote Druid column type; provide an explicit EXTEND schema");
        }
        if (type == null || !(type.isPrimitive() || type.isPrimitiveArray())) {
          throw new IOException("Unsupported remote Druid column type; provide an explicit EXTEND schema");
        }
        signature.add(name, type);
      }
      final RowSignature resolved = signature.build();
      require(ColumnType.LONG.equals(resolved.getColumnType("__time").orElse(null)));
      return resolved;
    }
    finally {
      Files.deleteIfExists(response.toPath());
    }
  }

  private static long timestamp(final JsonNode value) throws IOException
  {
    if (value.isIntegralNumber() && value.canConvertToLong()) {
      return value.longValue();
    }
    require(value.isTextual());
    return org.apache.druid.java.util.common.DateTimes.of(value.textValue()).getMillis();
  }

  File scan(
      final String dataSource,
      final Interval interval,
      final List<String> columns,
      @Nullable final DimFilter filter,
      final File directory
  ) throws IOException
  {
    final Map<String, Object> query = new LinkedHashMap<>();
    query.put("queryType", "scan");
    query.put("dataSource", dataSource);
    query.put("intervals", List.of(interval.toString()));
    query.put("columns", columns);
    query.put("resultFormat", "compactedList");
    query.put("order", "none");
    query.put("legacy", false);
    query.put("batchSize", 4096);
    if (filter != null) {
      query.put("filter", mapper.readTree(mapper.writerFor(DimFilter.class).writeValueAsBytes(filter)));
    }
    final File response = download(query, directory, connection.getMaxResponseBytes());
    File rows = null;
    boolean success = false;
    try {
      rows = Files.createTempFile(directory.toPath(), "remote-druid-rows-", ".json").toFile();
      normalize(response, rows, columns, interval);
      success = true;
      return rows;
    }
    finally {
      Files.deleteIfExists(response.toPath());
      if (!success && rows != null) {
        Files.deleteIfExists(rows.toPath());
      }
    }
  }

  private void normalize(final File response, final File rows, final List<String> columns, final Interval interval)
      throws IOException
  {
    final JsonFactory factory = responseFactory(true);
    try (final InputStream in = new InterruptibleInputStream(Files.newInputStream(response.toPath()));
         final JsonParser parser = factory.createParser(in);
         final OutputStream out = new LimitedOutputStream(Files.newOutputStream(rows.toPath()), connection.getMaxResponseBytes());
         final JsonGenerator writer = mapper.getFactory().createGenerator(out)) {
      writer.setRootValueSeparator(null);
      require(parser.nextToken() == JsonToken.START_ARRAY);
      final Set<String> responseFields = new HashSet<>();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        checkInterrupted();
        require(parser.currentToken() == JsonToken.START_OBJECT);
        List<String> batchColumns = null;
        boolean hasEvents = false;
        final Set<String> fields = new HashSet<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          require(parser.currentToken() == JsonToken.FIELD_NAME);
          final String field = parser.currentName();
          require(fields.add(field));
          responseFields.add(field);
          require(responseFields.size() <= 32);
          parser.nextToken();
          if ("columns".equals(field)) {
            final JsonNode names = readBoundedArray(parser);
            require(names.isArray());
            batchColumns = new ArrayList<>();
            for (final JsonNode name : names) {
              require(name.isTextual());
              batchColumns.add(name.textValue());
            }
            require(batchColumns.size() == columns.size() && Set.copyOf(batchColumns).equals(Set.copyOf(columns)));
          } else if ("events".equals(field)) {
            require(batchColumns != null && parser.currentToken() == JsonToken.START_ARRAY);
            hasEvents = true;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
              checkInterrupted();
              final JsonNode values = readBoundedArray(parser);
              require(values != null && values.isArray() && values.size() == batchColumns.size());
              final JsonNode time = values.get(batchColumns.indexOf("__time"));
              require(time.isIntegralNumber() && time.canConvertToLong() && interval.contains(time.longValue()));
              writer.writeStartObject();
              for (int i = 0; i < batchColumns.size(); i++) {
                writer.writeFieldName(batchColumns.get(i));
                mapper.writeTree(writer, values.get(i));
              }
              writer.writeEndObject();
              writer.writeRaw('\n');
            }
          } else {
            skipValue(parser, responseFields);
          }
        }
        require(hasEvents && batchColumns != null);
      }
      require(parser.nextToken() == null);
      writer.flush();
    }
    catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IOException("Invalid remote Druid scan response");
    }
  }

  private JsonFactory responseFactory(final boolean canonicalize)
  {
    final JsonFactory factory = mapper.getFactory().copy();
    // Normalization needs the UTF-8 parser's byte offsets and bounds field names separately.
    // Raw validation must not cache arbitrary field names across the complete response.
    if (!canonicalize) {
      factory.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);
    }
    factory.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
    factory.setStreamReadConstraints(factory.streamReadConstraints().rebuild()
                                            .maxStringLength(Math.min(factory.streamReadConstraints().getMaxStringLength(), MAX_ROW_BYTES))
                                            .maxNameLength(Math.min(factory.streamReadConstraints().getMaxNameLength(), MAX_ROW_BYTES))
                                            .maxNestingDepth(Math.min(factory.streamReadConstraints().getMaxNestingDepth(), 64))
                                            .build());
    return factory;
  }

  /** Bound both encoded size and object count before constructing a row tree. */
  private JsonNode readBoundedArray(final JsonParser parser) throws IOException
  {
    require(parser.currentToken() == JsonToken.START_ARRAY);
    final long start = parser.currentTokenLocation().getByteOffset();
    if (start < 0) {
      throw new NonRetryableException("Remote Druid scan responses must use UTF-8 JSON");
    }
    int depth = 0;
    int tokens = 0;
    try (final TokenBuffer buffer = new TokenBuffer(parser)) {
      do {
        checkInterrupted();
        if (++tokens > MAX_ROW_TOKENS) {
          throw new NonRetryableException("Remote Druid row or column list exceeds 100000 JSON tokens");
        }
        final JsonToken token = parser.currentToken();
        require(token != null && token != JsonToken.START_OBJECT);
        if (token.isStructStart()) {
          depth++;
        } else if (token.isStructEnd()) {
          depth--;
        }
        buffer.copyCurrentEvent(parser);
        if (parser.currentLocation().getByteOffset() - start > MAX_ROW_BYTES) {
          throw new NonRetryableException("Remote Druid row or column list exceeds 1 MiB");
        }
        if (depth > 0) {
          parser.nextToken();
        }
      } while (depth > 0);
      try (final JsonParser rowParser = buffer.asParser(mapper)) {
        return mapper.readTree(rowParser);
      }
    }
  }

  private File download(final Map<String, Object> query, @Nullable final File directory, final long maxBytes)
      throws IOException
  {
    for (int attempt = 0; ; attempt++) {
      checkInterrupted();
      final String queryId = UUID.randomUUID().toString();
      final Map<String, Object> request = new LinkedHashMap<>(query);
      request.put("context", Map.of(
          "queryId", queryId, "timeout", connection.getReadTimeout(), "returnPartialResults", false,
          "useCache", false, "populateCache", false, "useResultLevelCache", false, "populateResultLevelCache", false
      ));
      final File file = directory == null
                        ? Files.createTempFile("remote-druid-response-", ".json").toFile()
                        : Files.createTempFile(directory.toPath(), "remote-druid-response-", ".json").toFile();
      boolean success = false;
      try (final RemoteDruidHttpClient.Response http = httpClient.execute(
          connection.getEndpoint(), "POST", mapper.writeValueAsBytes(request)
      )) {
        final int status = http.status();
        if (status != 200) {
          if (status != 408 && status != 429 && status < 500) {
            throw new NonRetryableException("Remote Druid query failed with HTTP status[" + status + ']');
          }
          throw new IOException("Remote Druid query failed with HTTP status[" + status + ']');
        }
        try (final InputStream in = http.body(); final OutputStream out = Files.newOutputStream(file.toPath())) {
          final byte[] buffer = new byte[8192];
          long total = 0;
          int read;
          while ((read = in.read(buffer)) != -1) {
            checkInterrupted();
            if (read > maxBytes - total) {
              throw new NonRetryableException(
                  "segmentMetadata".equals(query.get("queryType"))
                  ? "Remote Druid metadata exceeds its response limit; provide an explicit EXTEND schema"
                  : "Remote Druid response exceeds maxResponseBytes; use smaller intervals"
              );
            }
            total += read;
            out.write(buffer, 0, read);
          }
        }
        try (final InputStream in = new InterruptibleInputStream(Files.newInputStream(file.toPath()));
             final JsonParser parser = responseFactory(false).createParser(in)) {
          require(parser.nextToken() == JsonToken.START_ARRAY);
          skipValue(parser, null);
          checkInterrupted();
          require(parser.nextToken() == null);
        }
        catch (com.fasterxml.jackson.core.exc.StreamConstraintsException e) {
          throw new NonRetryableException("Remote Druid response exceeds JSON parser limits");
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) {
          throw new IOException("Incomplete remote Druid response");
        }
        success = true;
        return file;
      }
      catch (IOException e) {
        if (e instanceof NonRetryableException || Thread.currentThread().isInterrupted() || attempt >= connection.getMaxRetries()) {
          throw e;
        }
        try {
          Thread.sleep(Math.min(1000L, 100L << attempt));
        }
        catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Remote Druid read interrupted");
        }
      }
      finally {
        if (!success) {
          Files.deleteIfExists(file.toPath());
          cancel(queryId);
        }
      }
    }
  }

  private static void skipValue(final JsonParser parser, @Nullable final Set<String> fieldNames) throws IOException
  {
    checkInterrupted();
    if (parser.currentToken() == JsonToken.VALUE_STRING) {
      parser.finishToken();
    }
    if (parser.currentToken().isStructStart()) {
      int depth = 1;
      while (depth > 0) {
        checkInterrupted();
        final JsonToken token = parser.nextToken();
        require(token != null);
        if (token == JsonToken.VALUE_STRING) {
          parser.finishToken();
        }
        if (token == JsonToken.FIELD_NAME && fieldNames != null) {
          fieldNames.add(parser.currentName());
          require(fieldNames.size() <= 32);
        }
        if (token.isStructStart()) {
          depth++;
        } else if (token.isStructEnd()) {
          depth--;
        }
      }
    }
  }

  private void cancel(final String queryId)
  {
    try (final RemoteDruidHttpClient.Response ignored = httpClient.execute(
        java.net.URI.create(connection.getEndpoint() + "/" + queryId), "DELETE", null
    )) {
      // Best effort. The source query also has a bounded timeout.
    }
    catch (Exception ignored) {
      // Preserve the original failure.
    }
  }

  private static void require(final boolean valid) throws IOException
  {
    if (!valid) {
      throw new IOException("Invalid remote Druid response structure");
    }
  }

  private static void checkInterrupted() throws InterruptedIOException
  {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Remote Druid read interrupted");
    }
  }

  /** Checks cancellation during parser refills, including a single long string or whitespace token. */
  static class InterruptibleInputStream extends FilterInputStream
  {
    InterruptibleInputStream(final InputStream in)
    {
      super(in);
    }

    @Override
    public int read() throws IOException
    {
      checkInterrupted();
      return in.read();
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException
    {
      checkInterrupted();
      return in.read(bytes, offset, Math.min(length, 8192));
    }
  }

  private static class LimitedOutputStream extends FilterOutputStream
  {
    private final long limit;
    private long written;

    LimitedOutputStream(final OutputStream out, final long limit)
    {
      super(out);
      this.limit = limit;
    }

    @Override
    public void write(final int value) throws IOException
    {
      if (written >= limit) {
        throw new NonRetryableException("Remote Druid rows exceed maxResponseBytes; use smaller intervals");
      }
      out.write(value);
      written++;
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException
    {
      if (length > limit - written) {
        throw new NonRetryableException("Remote Druid rows exceed maxResponseBytes; use smaller intervals");
      }
      out.write(bytes, offset, length);
      written += length;
    }
  }

  private static class NonRetryableException extends IOException
  {
    NonRetryableException(final String message)
    {
      super(message);
    }
  }
}
