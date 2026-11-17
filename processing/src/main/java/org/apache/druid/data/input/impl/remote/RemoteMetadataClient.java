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

package org.apache.druid.data.input.impl.remote;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Discovers remote column types and time bounds using bounded metadata requests; it does not read datasource rows.
 */
class RemoteMetadataClient implements AutoCloseable
{
  static final int MAX_METADATA_BYTES = 1024 * 1024;
  /**
   * Metadata queries run during SQL planning, so they use a short timeout regardless of the read timeout.
   */
  static final int MAX_METADATA_READ_TIMEOUT_MILLIS = 30_000;

  private final RemoteConnection connection;
  private final ObjectMapper mapper;
  private final int readTimeout;
  private final RemoteHttpClient httpClient;

  RemoteMetadataClient(final RemoteConnection connection, final ObjectMapper mapper)
  {
    this.connection = connection;
    this.mapper = mapper;
    this.readTimeout = Math.min(connection.getReadTimeout(), MAX_METADATA_READ_TIMEOUT_MILLIS);
    this.httpClient = connection.createClient(readTimeout);
  }

  @Override
  public void close()
  {
    httpClient.close();
  }

  RowSignature discoverSchema(final String dataSource) throws IOException
  {
    final JsonNode result = download(Map.of(
        "queryType", "segmentMetadata", "dataSource", dataSource,
        "intervals", List.of(Intervals.ETERNITY.toString()), "merge", true, "analysisTypes", List.of()
    ));
    if (result.isEmpty()) {
      throw new RemoteSchemaException(
          StringUtils.format("Remote Druid datasource[%s] was not found or has no segments", dataSource)
      );
    }
    require(result.size() == 1 && result.get(0).path("columns").isObject());
    final JsonNode columns = result.get(0).get("columns");
    final RowSignature.Builder signature = RowSignature.builder();
    final List<String> names = new ArrayList<>();
    columns.fieldNames().forEachRemaining(names::add);
    names.sort(Comparator.comparing((String name) -> !"__time".equals(name)).thenComparing(name -> name));
    for (final String name : names) {
      final JsonNode column = columns.get(name);
      if (column.hasNonNull("errorMessage") && !column.get("errorMessage").asText().isEmpty()) {
        throw new RemoteSchemaException("Remote Druid column metadata is ambiguous; provide an explicit EXTEND schema");
      }
      final JsonNode typeNode = column.hasNonNull("typeSignature") ? column.get("typeSignature") : column.get("type");
      require(typeNode != null && typeNode.isTextual());
      final ColumnType type;
      try {
        type = ColumnType.fromString(typeNode.textValue());
      }
      catch (RuntimeException e) {
        throw new RemoteSchemaException("Unsupported remote Druid column type; provide an explicit EXTEND schema");
      }
      if (type == null || !(type.isPrimitive() || type.isPrimitiveArray())) {
        throw new RemoteSchemaException("Unsupported remote Druid column type; provide an explicit EXTEND schema");
      }
      signature.add(name, type);
    }
    final RowSignature resolved = signature.build();
    if (!ColumnType.LONG.equals(resolved.getColumnType("__time").orElse(null))) {
      throw new RemoteSchemaException("Remote Druid datasource has no __time column of type LONG");
    }
    return resolved;
  }

  /**
   * Returns the half-open interval covering every row of the remote datasource, or null if it has no rows.
   */
  @Nullable
  Interval discoverTimeBounds(final String dataSource) throws IOException
  {
    final JsonNode result = download(Map.of("queryType", "timeBoundary", "dataSource", dataSource));
    if (result.isEmpty()) {
      return null;
    }
    try {
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
  }

  private JsonFactory responseFactory()
  {
    final JsonFactory original = mapper.getFactory();
    final StreamReadConstraints constraints = original.streamReadConstraints();

    // Metadata validation must not cache arbitrary field names across the complete response.
    final JsonFactory factory = new JsonFactoryBuilder(original)
        .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
        .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
        .streamReadConstraints(
            constraints.rebuild()
                       .maxStringLength(Math.min(constraints.getMaxStringLength(), MAX_METADATA_BYTES))
                       .maxNameLength(Math.min(constraints.getMaxNameLength(), MAX_METADATA_BYTES))
                       .maxNestingDepth(Math.min(constraints.getMaxNestingDepth(), 64))
                       .build()
        )
        .build();
    factory.setCodec(mapper);
    return factory;
  }

  /**
   * Runs a metadata query and returns its result, which must be a JSON array of at most {@link #MAX_METADATA_BYTES}.
   */
  private JsonNode download(final Map<String, Object> query) throws IOException
  {
    for (int attempt = 0; ; attempt++) {
      checkInterrupted();
      final String queryId = UUID.randomUUID().toString();
      final Map<String, Object> request = new LinkedHashMap<>(query);
      request.put("context", Map.of(
          "queryId", queryId, "timeout", readTimeout, "returnPartialResults", false,
          "useCache", false, "populateCache", false, "useResultLevelCache", false, "populateResultLevelCache", false
      ));

      final byte[] requestBytes = mapper.writeValueAsBytes(request);

      // Whether the source may still be running the query, which it may start even if its response never arrives. A
      // query that was rejected, or never sent, is not canceled.
      boolean sourceRunning = true;
      try (final RemoteHttpClient.Response http = httpClient.execute(connection.getEndpoint(), "POST", requestBytes)) {
        final int status = http.status();
        if (status != 200) {
          sourceRunning = false;
          try (final InputStream in = http.body()) {
            throw new RemoteStatusException(
                connection.getEndpoint(),
                status,
                RemoteStatusException.readMessage(in, mapper)
            );
          }
        }
        final JsonNode result;
        final LimitedInputStream body = new LimitedInputStream(http.body(), MAX_METADATA_BYTES);
        try (body; final JsonParser parser = responseFactory().createParser(body)) {
          require(parser.nextToken() == JsonToken.START_ARRAY);
          result = parser.readValueAsTree();
          checkInterrupted();
          require(parser.nextToken() == null);
        }
        catch (StreamConstraintsException e) {
          throw new RemoteNonRetryableException("Remote Druid response exceeds JSON parser limits");
        }
        catch (JsonProcessingException e) {
          // The source's response is not valid JSON, so asking again would return the same one.
          throw new RemoteNonRetryableException("Invalid remote Druid response: " + e.getOriginalMessage());
        }
        finally {
          // A source that sent its whole response has finished the query, so there is nothing to cancel.
          sourceRunning = !body.isEnded();
        }
        return result;
      }
      catch (IOException e) {
        if (sourceRunning && !(e instanceof RemoteHttpClient.NotSentException)) {
          cancel(queryId);
        }
        if (!isRetryable(e) || attempt >= connection.getMaxRetries()) {
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
      catch (RuntimeException e) {
        if (sourceRunning) {
          cancel(queryId);
        }
        throw e;
      }
    }
  }

  private static boolean isRetryable(final IOException e)
  {
    return !(e instanceof RemoteNonRetryableException)
           && !(e instanceof RemoteSchemaException)
           && !(e instanceof RemoteStatusException status && !status.isRetryable())
           && !(e instanceof RemoteHttpClient.ConfigurationException)
           && !(e instanceof InterruptedIOException)
           && !Thread.currentThread().isInterrupted();
  }

  private void cancel(final String queryId)
  {
    connection.cancel(connection.getEndpoint(), queryId);
  }

  private static void require(final boolean valid) throws IOException
  {
    if (!valid) {
      throw new RemoteNonRetryableException("Invalid remote Druid response structure");
    }
  }

  private static void checkInterrupted() throws InterruptedIOException
  {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Remote Druid read interrupted");
    }
  }

  /**
   * Fails a metadata response once it exceeds its byte limit, and stops reading it once the thread is interrupted.
   */
  private static class LimitedInputStream extends FilterInputStream
  {
    private final long maxBytes;
    private long total;
    private boolean ended;

    LimitedInputStream(final InputStream in, final long maxBytes)
    {
      super(in);
      this.maxBytes = maxBytes;
    }

    @Override
    public int read() throws IOException
    {
      checkInterrupted();
      final int b = super.read();
      if (b >= 0) {
        count(1);
      } else {
        ended = true;
      }
      return b;
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException
    {
      checkInterrupted();
      final int read = super.read(bytes, offset, length);
      if (read > 0) {
        count(read);
      } else if (read < 0) {
        ended = true;
      }
      return read;
    }

    @Override
    public long skip(final long n)
    {
      throw new UnsupportedOperationException();
    }

    /**
     * Whether the whole response has been read.
     */
    boolean isEnded()
    {
      return ended;
    }

    private void count(final int read) throws IOException
    {
      total += read;
      if (total > maxBytes) {
        throw new RemoteSchemaException(
            "Remote Druid metadata exceeds its response limit; provide an explicit EXTEND schema"
        );
      }
    }
  }
}
