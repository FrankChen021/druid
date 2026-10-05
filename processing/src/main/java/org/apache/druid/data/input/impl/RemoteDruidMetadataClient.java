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
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/// Discovers remote column types and time bounds using bounded metadata requests; it does not read datasource rows.
class RemoteDruidMetadataClient implements AutoCloseable
{
  private static final Logger LOG = new Logger(RemoteDruidMetadataClient.class);

  static final int MAX_METADATA_BYTES = 1024 * 1024;

  private final RemoteDruidConnection connection;
  private final ObjectMapper mapper;
  private final RemoteDruidHttpClient httpClient;

  RemoteDruidMetadataClient(final RemoteDruidConnection connection, final ObjectMapper mapper)
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

  RowSignature discoverSchema(final String dataSource) throws IOException
  {
    final File response = download(Map.of(
        "queryType", "segmentMetadata", "dataSource", dataSource,
        "intervals", List.of(Intervals.ETERNITY.toString()), "merge", true, "analysisTypes", List.of()
    ), MAX_METADATA_BYTES);
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

  /// Returns the half-open interval covering every row of the remote datasource, or null if it has no rows.
  @Nullable
  Interval discoverTimeBounds(final String dataSource) throws IOException
  {
    final File response = download(
        Map.of("queryType", "timeBoundary", "dataSource", dataSource),
        MAX_METADATA_BYTES
    );
    try {
      final JsonNode result = mapper.readTree(response);
      require(result.isArray());
      if (result.isEmpty()) {
        return null;
      }
      final JsonNode bounds = result.get(0).path("result");
      require(bounds.path("minTime").isTextual() && bounds.path("maxTime").isTextual());
      final DateTime minTime = DateTimes.of(bounds.get("minTime").textValue());
      final DateTime maxTime = DateTimes.of(bounds.get("maxTime").textValue());
      require(!maxTime.isBefore(minTime));
      return new Interval(minTime, maxTime.plus(1));
    }
    catch (IllegalArgumentException e) {
      throw new IOException("Invalid remote Druid time boundary response");
    }
    finally {
      Files.deleteIfExists(response.toPath());
    }
  }

  private JsonFactory responseFactory()
  {
    final JsonFactory original = mapper.getFactory();
    // Metadata validation must not cache arbitrary field names across the complete response.
    final JsonFactory factory = new JsonFactoryBuilder(original)
        .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
        .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
        .streamReadConstraints(original.streamReadConstraints().rebuild()
                                       .maxStringLength(Math.min(original.streamReadConstraints().getMaxStringLength(), MAX_METADATA_BYTES))
                                       .maxNameLength(Math.min(original.streamReadConstraints().getMaxNameLength(), MAX_METADATA_BYTES))
                                       .maxNestingDepth(Math.min(original.streamReadConstraints().getMaxNestingDepth(), 64))
                                       .build())
        .build();
    factory.setCodec(mapper);
    return factory;
  }

  private File download(final Map<String, Object> query, final long maxBytes)
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
      final File file = Files.createTempFile("remote-druid-metadata-", ".json").toFile();
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
                  "Remote Druid metadata exceeds its response limit; provide an explicit EXTEND schema"
              );
            }
            total += read;
            out.write(buffer, 0, read);
          }
        }
        try (final InputStream in = new InterruptibleInputStream(Files.newInputStream(file.toPath()));
             final JsonParser parser = responseFactory().createParser(in)) {
          require(parser.nextToken() == JsonToken.START_ARRAY);
          skipValue(parser);
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

  private static void skipValue(final JsonParser parser) throws IOException
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
      // Preserve the original failure and avoid logging provider exceptions that may contain credentials.
      LOG.debug("Unable to cancel remote Druid query[%s]", queryId);
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

  /// Checks cancellation during parser refills, including a single long string or whitespace token.
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

  private static class NonRetryableException extends IOException
  {
    NonRetryableException(final String message)
    {
      super(message);
    }
  }
}
