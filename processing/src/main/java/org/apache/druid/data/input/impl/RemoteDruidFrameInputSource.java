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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/// Reads one source MSQ result partition through the versioned remote-frame gateway.
@JsonTypeName(RemoteDruidFrameInputSource.TYPE_KEY)
public class RemoteDruidFrameInputSource extends AbstractInputSource implements SplittableInputSource<RemoteDruidFrameInputSource.PartitionSplit>
{
  public static final String TYPE_KEY = "remoteDruidFrames";
  public static final int PROTOCOL_VERSION = 1;
  public static final int FRAME_FILE_VERSION = 1;
  public static final long LEASE_MILLIS = TimeUnit.MINUTES.toMillis(2);
  private static final long MAX_SESSION_WAIT_MILLIS = TimeUnit.HOURS.toMillis(24) + TimeUnit.MINUTES.toMillis(5);
  private static final long POLL_INTERVAL_MILLIS = 250;

  private final RemoteDruidConnection connection;
  @Nullable
  private final String sql;
  @Nullable
  private final String clientRequestId;
  private final Map<String, Object> sourceContext;
  @Nullable
  private final RowSignature expectedSignature;
  @Nullable
  private final String sessionId;
  @Nullable
  private final String partitionId;
  @Nullable
  private final String attemptId;
  @Nullable
  private final RowSignature partitionSignature;
  @Nullable
  private final Map<String, List<String>> partitionColumnMapping;
  private final HttpInputSourceConfig config;
  private final ObjectMapper mapper;

  @JsonCreator
  public RemoteDruidFrameInputSource(
      @JsonProperty("connection") final RemoteDruidConnection connection,
      @JsonProperty("sql") @Nullable final String sql,
      @JsonProperty("clientRequestId") @Nullable final String clientRequestId,
      @JsonProperty("sourceContext") @Nullable final Map<String, Object> sourceContext,
      @JsonProperty("expectedSignature") @Nullable final RowSignature expectedSignature,
      @JsonProperty("sessionId") @Nullable final String sessionId,
      @JsonProperty("partitionId") @Nullable final String partitionId,
      @JsonProperty("attemptId") @Nullable final String attemptId,
      @JsonProperty("partitionSignature") @Nullable final RowSignature partitionSignature,
      @JsonProperty("partitionColumnMapping") @Nullable final Map<String, List<String>> partitionColumnMapping,
      @JacksonInject final HttpInputSourceConfig config,
      @JacksonInject @Json final ObjectMapper mapper
  )
  {
    this.connection = Preconditions.checkNotNull(connection, "connection");
    this.sql = sql;
    this.clientRequestId = clientRequestId;
    this.sourceContext = sourceContext == null ? Map.of() : Map.copyOf(sourceContext);
    this.expectedSignature = expectedSignature;
    this.sessionId = sessionId;
    this.partitionId = partitionId;
    this.attemptId = attemptId;
    this.partitionSignature = partitionSignature;
    this.partitionColumnMapping = copyColumnMapping(partitionColumnMapping);
    this.config = Preconditions.checkNotNull(config, "config");
    this.mapper = Preconditions.checkNotNull(mapper, "mapper");
    Preconditions.checkArgument(
        (sessionId == null && sql != null && clientRequestId != null)
        || (sessionId != null && partitionId != null && attemptId != null && partitionSignature != null),
        "Remote frame source must describe either a query or a bound result partition"
    );
    Preconditions.checkArgument(
        Set.of("http", "https").contains(connection.getEndpoint().getScheme().toLowerCase(Locale.ROOT)),
        "Remote Druid supports HTTP(S) endpoints"
    );
    HttpInputSource.throwIfInvalidProtocols(config, List.of(connection.getEndpoint()));
  }

  @JsonProperty
  public RemoteDruidConnection getConnection()
  {
    return connection;
  }

  @JsonProperty
  @Nullable
  public String getSql()
  {
    return sql;
  }

  @JsonProperty
  @Nullable
  public String getClientRequestId()
  {
    return clientRequestId;
  }

  @JsonProperty
  public Map<String, Object> getSourceContext()
  {
    return sourceContext;
  }

  @JsonProperty
  @Nullable
  public RowSignature getExpectedSignature()
  {
    return expectedSignature;
  }

  @JsonProperty
  @Nullable
  public String getSessionId()
  {
    return sessionId;
  }

  @JsonProperty
  @Nullable
  public String getPartitionId()
  {
    return partitionId;
  }

  @JsonProperty
  @Nullable
  public String getAttemptId()
  {
    return attemptId;
  }

  @JsonProperty
  @Nullable
  public RowSignature getPartitionSignature()
  {
    return partitionSignature;
  }

  @JsonProperty
  @Nullable
  public Map<String, List<String>> getPartitionColumnMapping()
  {
    return partitionColumnMapping;
  }

  /// Releases a submitted source query session through the authenticated frame API.
  public void releaseSession(final String sourceSessionId) throws IOException
  {
    try (final RemoteDruidFrameClient client = new RemoteDruidFrameClient(connection, mapper)) {
      client.release(sourceSessionId);
    }
  }

  /// Renews a source query lease through this input source's authenticated frame API.
  public void renewSession(final String sourceSessionId, final long leaseMillis) throws IOException
  {
    try (final RemoteDruidFrameClient client = new RemoteDruidFrameClient(connection, mapper)) {
      client.renewLease(sourceSessionId, leaseMillis);
    }
  }

  @JsonIgnore
  @Override
  public Set<String> getTypes()
  {
    return Set.of(TYPE_KEY);
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  public int estimateNumSplits(final InputFormat inputFormat, @Nullable final SplitHintSpec splitHintSpec)
  {
    return 1;
  }

  @Override
  public Stream<InputSplit<PartitionSplit>> createSplits(
      @Nullable final InputFormat inputFormat,
      @Nullable final SplitHintSpec splitHintSpec
  ) throws IOException
  {
    Preconditions.checkState(sessionId == null, "A bound remote frame partition cannot be split again");
    try (final RemoteDruidFrameClient client = new RemoteDruidFrameClient(connection, mapper)) {
      final SessionInfo session = client.submitAndWait(sql, clientRequestId, sourceContext);
      final Manifest manifest = session.manifest;
      if (expectedSignature != null && !matchesExpectedSignature(expectedSignature, manifest)) {
        client.release(session.queryId);
        throw new IOException("Remote frame source row signature differs from the planned target signature");
      }
      if (manifest.partitions.isEmpty()) {
        client.release(session.queryId);
        return Stream.empty();
      }
      final Map<String, List<String>> columnMapping = manifest.columnMapping == null || manifest.columnMapping.isEmpty()
                                                      ? identityColumnMapping(manifest.signature)
                                                      : copyColumnMapping(manifest.columnMapping);
      return manifest.partitions.stream()
                               .map(partition -> new InputSplit<>(new PartitionSplit(
                                   session.queryId,
                                   partition.id,
                                   partition.attemptId,
                                   manifest.signature,
                                   columnMapping
                               )));
    }
  }

  @Override
  public RemoteDruidFrameInputSource withSplit(final InputSplit<PartitionSplit> split)
  {
    final PartitionSplit partition = Preconditions.checkNotNull(split.get(), "split");
    return new RemoteDruidFrameInputSource(
        connection,
        null,
        null,
        Map.of(),
        null,
        partition.sessionId,
        partition.partitionId,
        partition.attemptId,
        partition.signature,
        partition.columnMapping,
        config,
        mapper
    );
  }

  @Override
  protected InputSourceReader fixedFormatReader(final InputRowSchema schema, final File temporaryDirectory)
  {
    Preconditions.checkState(sessionId != null, "Remote frame input must be bound to a source result partition");
    Preconditions.checkArgument(schema != null, "inputRowSchema is required");
    Preconditions.checkArgument(partitionSignature != null, "partitionSignature is required");

    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read(@Nullable final InputStats inputStats) throws IOException
      {
        final File frameFile = downloadPartition(temporaryDirectory, inputStats);
        final FrameFile frames;
        try {
          frames = FrameFile.open(frameFile, null);
        }
        catch (IOException | RuntimeException e) {
          Files.deleteIfExists(frameFile.toPath());
          throw e;
        }

        return new FrameRowsIterator(
            frames,
            frameFile,
            partitionSignature,
            partitionColumnMapping == null ? identityColumnMapping(partitionSignature) : partitionColumnMapping,
            schema
        );
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
      {
        return read(null).map(row -> {
          final Map<String, Object> raw = new LinkedHashMap<>();
          for (final String column : partitionSignature.getColumnNames()) {
            raw.put(column, row.getRaw(column));
          }
          return InputRowListPlusRawValues.of(row, raw);
        });
      }
    };
  }

  private File downloadPartition(final File directory, @Nullable final InputStats inputStats) throws IOException
  {
    Files.createDirectories(directory.toPath());
    final File file = Files.createTempFile(directory.toPath(), "remote-druid-frame-", ".frames").toFile();
    boolean complete = false;
    long offset = 0;
    try (final RemoteDruidFrameClient client = new RemoteDruidFrameClient(connection, mapper)) {
      boolean lastFetch = false;
      while (!lastFetch) {
        client.renewLease(sessionId, LEASE_MILLIS);
        IOException fetchException = null;
        for (int attempt = 0; attempt <= connection.getMaxRetries(); attempt++) {
          final long offsetBeforeAttempt = offset;
          try (final RemoteDruidHttpClient.Response response = client.fetchPartition(
              sessionId,
              partitionId,
              attemptId,
              offset
          )) {
            if (response.status() != 200) {
              throw new IOException("Remote frame partition request failed with HTTP status [" + response.status() + "]");
            }
            try (final InputStream input = response.body();
                 final FileOutputStream output = new FileOutputStream(file, true)) {
              final byte[] buffer = new byte[8192];
              int count;
              while ((count = input.read(buffer)) >= 0) {
                if (count == 0) {
                  continue;
                }
                final long nextOffset = Math.addExact(offset, count);
                if (nextOffset > connection.getMaxResponseBytes()) {
                  throw new IOException("Remote frame partition exceeded maxResponseBytes");
                }
                output.write(buffer, 0, count);
                offset = nextOffset;
              }
            }
            lastFetch = FrameFileHttpResponseHandler.HEADER_LAST_FETCH_VALUE.equals(
                response.header(FrameFileHttpResponseHandler.HEADER_LAST_FETCH_NAME)
            );
            if (!lastFetch && offset == offsetBeforeAttempt) {
              throw new IOException("Remote frame partition returned an empty non-final chunk");
            }
            fetchException = null;
            break;
          }
          catch (IOException e) {
            try (final RandomAccessFile partialFile = new RandomAccessFile(file, "rw")) {
              if (partialFile.length() < offset) {
                throw new IOException("Remote frame partition file is shorter than its accepted byte offset");
              }
              partialFile.setLength(offset);
            }
            catch (IOException truncateException) {
              e.addSuppressed(truncateException);
              throw e;
            }
            fetchException = e;
            if (attempt == connection.getMaxRetries()) {
              break;
            }
          }
        }
        if (fetchException != null) {
          throw fetchException;
        }
      }

      if (inputStats != null) {
        inputStats.incrementProcessedBytes(file.length());
      }
      complete = true;
      return file;
    }
    catch (ArithmeticException e) {
      throw new IOException("Remote frame partition size overflow", e);
    }
    finally {
      if (!complete) {
        Files.deleteIfExists(file.toPath());
      }
    }
  }

  private static class FrameRowsIterator implements CloseableIterator<InputRow>
  {
    private final FrameFile frames;
    private final File file;
    private final RowSignature signature;
    private final Map<String, List<String>> columnMapping;
    private final InputRowSchema schema;
    private int frameNumber;
    private int rowNumber;
    @Nullable
    private RowsAndColumns currentRows;
    private List<ColumnAccessor> accessors = List.of();
    private boolean closed;

    private FrameRowsIterator(
        final FrameFile frames,
        final File file,
        final RowSignature signature,
        final Map<String, List<String>> columnMapping,
        final InputRowSchema schema
    )
    {
      this.frames = frames;
      this.file = file;
      this.signature = signature;
      this.columnMapping = columnMapping;
      this.schema = schema;
    }

    @Override
    public boolean hasNext()
    {
      if (closed) {
        return false;
      }
      try {
        while (currentRows == null || rowNumber >= currentRows.numRows()) {
          if (frameNumber >= frames.numFrames()) {
            close();
            return false;
          }
          final RowsAndColumns rawRows = frames.rac(frameNumber++, null);
          final Frame frame = rawRows.as(Frame.class);
          if (frame == null) {
            throw new IOException("Remote frame file contains an unsupported rows-and-columns entry");
          }
          currentRows = frame.type().isRowBased()
                        ? new RowBasedFrameRowsAndColumns(frame, signature)
                        : new ColumnBasedFrameRowsAndColumns(frame, signature);
          rowNumber = 0;
          final List<ColumnAccessor> newAccessors = new ArrayList<>(signature.size());
          for (final String column : signature.getColumnNames()) {
            final org.apache.druid.query.rowsandcols.column.Column data = currentRows.findColumn(column);
            if (data == null) {
              throw new IOException("Remote frame is missing a column from its manifest signature");
            }
            newAccessors.add(data.toAccessor());
          }
          accessors = List.copyOf(newAccessors);
        }
        return true;
      }
      catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }

    @Override
    public InputRow next()
    {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      final Map<String, Object> event = new LinkedHashMap<>();
      for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
        final String queryColumn = signature.getColumnName(columnNumber);
        final Object value = accessors.get(columnNumber).getObject(rowNumber);
        for (final String outputColumn : columnMapping.getOrDefault(queryColumn, List.of(queryColumn))) {
          event.put(outputColumn, value);
        }
      }
      rowNumber++;
      return MapInputRowParser.parse(schema, event);
    }

    @Override
    public void close() throws IOException
    {
      if (!closed) {
        closed = true;
        try {
          frames.close();
        }
        finally {
          Files.deleteIfExists(file.toPath());
        }
      }
    }
  }

  /// One manifest partition and its immutable source attempt.
  public static class PartitionSplit
  {
    private final String sessionId;
    private final String partitionId;
    private final String attemptId;
    private final RowSignature signature;
    private final Map<String, List<String>> columnMapping;

    @JsonCreator
    public PartitionSplit(
        @JsonProperty("sessionId") final String sessionId,
        @JsonProperty("partitionId") final String partitionId,
        @JsonProperty("attemptId") final String attemptId,
        @JsonProperty("signature") final RowSignature signature,
        @JsonProperty("columnMapping") @Nullable final Map<String, List<String>> columnMapping
    )
    {
      this.sessionId = sessionId;
      this.partitionId = partitionId;
      this.attemptId = attemptId;
      this.signature = signature;
      this.columnMapping = copyColumnMapping(columnMapping);
    }

    @JsonProperty
    public String getSessionId()
    {
      return sessionId;
    }

    @JsonProperty
    public String getPartitionId()
    {
      return partitionId;
    }

    @JsonProperty
    public String getAttemptId()
    {
      return attemptId;
    }

    @JsonProperty
    public RowSignature getSignature()
    {
      return signature;
    }

    @JsonProperty
    public Map<String, List<String>> getColumnMapping()
    {
      return columnMapping == null ? Map.of() : columnMapping;
    }
  }

  private static class RemoteDruidFrameClient implements AutoCloseable
  {
    private final RemoteDruidConnection connection;
    private final ObjectMapper mapper;
    private final RemoteDruidHttpClient httpClient;

    private RemoteDruidFrameClient(final RemoteDruidConnection connection, final ObjectMapper mapper)
    {
      this.connection = connection;
      this.mapper = mapper;
      this.httpClient = connection.getAuthentication().createClient(connection.getConnectTimeout(), connection.getReadTimeout());
    }

    private SessionInfo submitAndWait(
        @Nullable final String sql,
        @Nullable final String requestId,
        final Map<String, Object> context
    ) throws IOException
    {
      final Map<String, Object> body = Map.of(
          "sql", Preconditions.checkNotNull(sql, "sql"),
          "clientRequestId", Preconditions.checkNotNull(requestId, "clientRequestId"),
          "protocolVersion", PROTOCOL_VERSION,
          "context", context
      );
      final byte[] requestBody = mapper.writeValueAsBytes(body);
      final SessionInfo submitted = submitWithRetry(requestBody);
      try {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_SESSION_WAIT_MILLIS);
        SessionInfo info = submitted;
        while (!"ready".equals(info.state)) {
          if ("failed".equals(info.state) || "cancelled".equals(info.state) || "expired".equals(info.state)
              || "released".equals(info.state)) {
            throw new IOException("Remote frame source query ended in state [" + info.state + "]");
          }
          if (System.nanoTime() >= deadline) {
            throw new IOException("Remote frame source query did not become ready before the client timeout");
          }
          try {
            Thread.sleep(POLL_INTERVAL_MILLIS);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("Remote frame source wait was interrupted");
          }
          info = request(sessionEndpoint(submitted.queryId), "GET", null, SessionInfo.class);
        }
        Preconditions.checkNotNull(info.manifest, "ready remote frame session manifest");
        if (info.manifest.protocolVersion != PROTOCOL_VERSION || info.manifest.frameFileVersion != FRAME_FILE_VERSION) {
          throw new IOException("Remote frame protocol or FrameFile version is not supported by this Druid version");
        }
        return info;
      }
      catch (IOException | RuntimeException e) {
        final boolean interrupted = Thread.interrupted();
        try {
          release(submitted.queryId);
        }
        catch (IOException releaseException) {
          e.addSuppressed(releaseException);
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
        throw e;
      }
    }

    private SessionInfo submitWithRetry(final byte[] requestBody) throws IOException
    {
      for (int attempt = 0; ; attempt++) {
        try {
          return request(sessionEndpoint(), "POST", requestBody, SessionInfo.class);
        }
        catch (IOException e) {
          if (isHttpStatusFailure(e) || attempt >= connection.getMaxRetries()) {
            throw e;
          }
          try {
            TimeUnit.MILLISECONDS.sleep(Math.min(1000L, 100L << attempt));
          }
          catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("Remote frame submission retry was interrupted");
          }
        }
      }
    }

    private static boolean isHttpStatusFailure(final IOException exception)
    {
      final String message = exception.getMessage();
      return message != null && (message.startsWith("Remote frame API request failed with HTTP status")
                                 || message.startsWith("Remote frame source rejected the request with HTTP status")
                                 || message.startsWith("Remote frame API was not found on the source"));
    }

    private RemoteDruidHttpClient.Response fetchPartition(
        final String session,
        final String partition,
        final String attempt,
        final long offset
    ) throws IOException
    {
      final String path = sessionEndpoint(session) + "/partitions/" + encode(partition)
                          + "?attemptId=" + encode(attempt) + "&offset=" + offset;
      return httpClient.execute(uri(path), "GET", null);
    }

    private void renewLease(final String session, final long leaseMillis) throws IOException
    {
      final byte[] body = mapper.writeValueAsBytes(Map.of("leaseMillis", leaseMillis));
      request(sessionEndpoint(session) + "/lease", "POST", body, Boolean.class);
    }

    private void release(final String session) throws IOException
    {
      try (final RemoteDruidHttpClient.Response response = httpClient.execute(uri(sessionEndpoint(session)), "DELETE", null)) {
        if (response.status() != 200 && response.status() != 202 && response.status() != 404) {
          throw new IOException("Remote frame session release failed with HTTP status [" + response.status() + "]");
        }
      }
    }

    private <T> T request(
        final String path,
        final String method,
        @Nullable final byte[] body,
        final Class<T> resultClass
    ) throws IOException
    {
      try (final RemoteDruidHttpClient.Response response = httpClient.execute(uri(path), method, body)) {
        if (response.status() < 200 || response.status() >= 300) {
          if (response.status() == 409) {
            throw new IOException(
                "Remote frame source rejected the request with HTTP status [409]; source and target protocol versions may be incompatible, "
                + "or this request ID may already be complete"
            );
          }
          if (response.status() == 404 && sessionEndpoint().equals(path)) {
            throw new IOException(
                "Remote frame API was not found on the source; upgrade the source and target to versions that support remote frame protocol ["
                + PROTOCOL_VERSION + "]"
            );
          }
          throw new IOException("Remote frame API request failed with HTTP status [" + response.status() + "]");
        }
        return mapper.readValue(response.body(), resultClass);
      }
    }

    private String sessionEndpoint()
    {
      return "/sql/remote-frames";
    }

    private String sessionEndpoint(final String session)
    {
      return sessionEndpoint() + "/" + encode(session);
    }

    private URI uri(final String path)
    {
      final String endpoint = connection.getEndpoint().toString();
      final String normalizedEndpoint = endpoint.endsWith("/")
                                       ? endpoint.substring(0, endpoint.length() - 1)
                                       : endpoint;
      return URI.create(normalizedEndpoint + path);
    }

    private static String encode(final String value)
    {
      return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public void close()
    {
      httpClient.close();
    }
  }

  private static class SessionInfo
  {
    private final String queryId;
    private final String state;
    @Nullable
    private final Manifest manifest;

    @JsonCreator
    private SessionInfo(
        @JsonProperty("queryId") final String queryId,
        @JsonProperty("state") final String state,
        @JsonProperty("manifest") @Nullable final Manifest manifest
    )
    {
      this.queryId = queryId;
      this.state = state;
      this.manifest = manifest;
    }
  }

  private static class Manifest
  {
    private final int protocolVersion;
    private final int frameFileVersion;
    private final RowSignature signature;
    @Nullable
    private final Map<String, List<String>> columnMapping;
    private final List<ManifestPartition> partitions;

    @JsonCreator
    private Manifest(
        @JsonProperty("protocolVersion") final int protocolVersion,
        @JsonProperty("frameFileVersion") final int frameFileVersion,
        @JsonProperty("signature") final RowSignature signature,
        @JsonProperty("columnMapping") @Nullable final Map<String, List<String>> columnMapping,
        @JsonProperty("partitions") final List<ManifestPartition> partitions
    )
    {
      this.protocolVersion = protocolVersion;
      this.frameFileVersion = frameFileVersion;
      this.signature = signature;
      this.columnMapping = copyColumnMapping(columnMapping);
      this.partitions = List.copyOf(partitions);
    }
  }

  private static Map<String, List<String>> copyColumnMapping(@Nullable final Map<String, List<String>> columnMapping)
  {
    if (columnMapping == null) {
      return null;
    }
    final Map<String, List<String>> copy = new LinkedHashMap<>();
    columnMapping.forEach((queryColumn, outputColumns) -> copy.put(queryColumn, List.copyOf(outputColumns)));
    return Map.copyOf(copy);
  }

  private static Map<String, List<String>> identityColumnMapping(final RowSignature signature)
  {
    final Map<String, List<String>> mapping = new LinkedHashMap<>();
    for (final String columnName : signature.getColumnNames()) {
      mapping.put(columnName, List.of(columnName));
    }
    return Map.copyOf(mapping);
  }

  private static boolean matchesExpectedSignature(final RowSignature expected, final Manifest manifest)
  {
    final Map<String, List<String>> mapping = manifest.columnMapping == null || manifest.columnMapping.isEmpty()
                                              ? identityColumnMapping(manifest.signature)
                                              : manifest.columnMapping;
    if (mapping.size() != manifest.signature.size()) {
      return false;
    }
    final Set<String> mappedOutputColumns = new java.util.HashSet<>();
    for (final String queryColumn : manifest.signature.getColumnNames()) {
      final org.apache.druid.segment.column.ColumnType queryType =
          manifest.signature.getColumnType(queryColumn).orElse(null);
      final List<String> outputColumns = mapping.get(queryColumn);
      if (outputColumns == null || outputColumns.isEmpty()) {
        return false;
      }
      for (final String outputColumn : outputColumns) {
        if (!mappedOutputColumns.add(outputColumn)
            || expected.getColumnType(outputColumn).filter(queryType::equals).isEmpty()) {
          return false;
        }
      }
    }
    return mappedOutputColumns.size() == expected.size()
           && mappedOutputColumns.containsAll(expected.getColumnNames());
  }

  private static class ManifestPartition
  {
    private final String id;
    private final String attemptId;

    @JsonCreator
    private ManifestPartition(
        @JsonProperty("id") final String id,
        @JsonProperty("attemptId") final String attemptId
    )
    {
      this.id = id;
      this.attemptId = attemptId;
    }
  }
}
