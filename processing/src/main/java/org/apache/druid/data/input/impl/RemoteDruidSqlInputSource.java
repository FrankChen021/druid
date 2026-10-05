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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/// Reads the streamed result of a SQL SELECT that SQL planning pushed down to a remote Druid cluster.
///
/// The source runs the query with its own engine (Dart by default) and streams `arrayLines` results, so the
/// source cluster needs neither tasks nor durable storage. When [#isTimeRangeParameters()] is true, the SQL has
/// two `BIGINT` parameters, `[start, end)` in epoch milliseconds, and each split reads one time range.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RemoteDruidSqlInputSource extends AbstractInputSource implements SplittableInputSource<Interval>
{
  public static final String TYPE_KEY = "remoteDruidSql";
  public static final String ENGINE_DART = "msq-dart";
  public static final String ENGINE_NATIVE = "native";
  public static final Set<String> ENGINES = Set.of(ENGINE_DART, ENGINE_NATIVE);
  static final int MAX_SPLITS = 10_000;
  static final int MAX_LINE_BYTES = 16 * 1024 * 1024;
  private static final int MAX_ERROR_BYTES = 64 * 1024;
  private static final Logger LOG = new Logger(RemoteDruidSqlInputSource.class);

  private final RemoteDruidConnection connection;
  private final String dataSource;
  private final String sql;
  private final String engine;
  private final Map<String, Object> context;
  private final RowSignature signature;
  private final List<String> timestampColumns;
  private final boolean timeRangeParameters;
  @Nullable
  private final List<Interval> intervals;
  @Nullable
  private final Period splitDuration;
  @Nullable
  private final Interval range;
  private final HttpInputSourceConfig config;
  private final ObjectMapper mapper;

  @JsonCreator
  public RemoteDruidSqlInputSource(
      @JsonProperty("connection") final RemoteDruidConnection connection,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("sql") final String sql,
      @JsonProperty("engine") @Nullable final String engine,
      @JsonProperty("context") @Nullable final Map<String, Object> context,
      @JsonProperty("signature") final RowSignature signature,
      @JsonProperty("timestampColumns") @Nullable final List<String> timestampColumns,
      @JsonProperty("timeRangeParameters") final boolean timeRangeParameters,
      @JsonProperty("intervals") @Nullable final List<Interval> intervals,
      @JsonProperty("splitDuration") @Nullable final Period splitDuration,
      @JsonProperty("range") @Nullable final Interval range,
      @JacksonInject final HttpInputSourceConfig config,
      @JacksonInject @Json final ObjectMapper mapper
  )
  {
    this.connection = Preconditions.checkNotNull(connection, "connection");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.sql = Preconditions.checkNotNull(sql, "sql");
    this.engine = engine == null ? ENGINE_DART : engine;
    this.context = context == null ? Map.of() : Map.copyOf(context);
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.timestampColumns = timestampColumns == null ? List.of() : List.copyOf(timestampColumns);
    this.timeRangeParameters = timeRangeParameters;
    this.intervals = intervals == null ? null : List.copyOf(intervals);
    this.splitDuration = splitDuration;
    this.range = range;
    this.config = Preconditions.checkNotNull(config, "config");
    this.mapper = Preconditions.checkNotNull(mapper, "mapper");
    Preconditions.checkArgument(ENGINES.contains(this.engine), "Unsupported remote Druid engine[%s]", this.engine);
    Preconditions.checkArgument(
        timeRangeParameters || (intervals == null && splitDuration == null),
        "intervals and splitDuration require timeRangeParameters"
    );
    Preconditions.checkArgument(
        splitDuration == null || DateTimes.EPOCH.plus(splitDuration).isAfter(DateTimes.EPOCH),
        "splitDuration must be positive"
    );
    Preconditions.checkArgument(
        Set.of("http", "https").contains(StringUtils.toLowerCase(connection.getEndpoint().getScheme())),
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
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getSql()
  {
    return sql;
  }

  @JsonProperty
  public String getEngine()
  {
    return engine;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @JsonProperty
  public RowSignature getSignature()
  {
    return signature;
  }

  @JsonProperty
  public List<String> getTimestampColumns()
  {
    return timestampColumns;
  }

  @JsonProperty
  public boolean isTimeRangeParameters()
  {
    return timeRangeParameters;
  }

  @JsonProperty
  @Nullable
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  @Nullable
  public Period getSplitDuration()
  {
    return splitDuration;
  }

  /// The time range read by a bound split; null before splitting.
  @JsonProperty
  @Nullable
  public Interval getRange()
  {
    return range;
  }

  @JsonIgnore
  @Override
  public Set<String> getTypes()
  {
    // Authorized under the same input source type as the DRUID table function.
    return Set.of(RemoteDruidInputSource.TYPE_KEY);
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  public Stream<InputSplit<Interval>> createSplits(
      @Nullable final InputFormat inputFormat,
      @Nullable final SplitHintSpec splitHintSpec
  ) throws IOException
  {
    if (range != null) {
      return Stream.of(new InputSplit<>(range));
    }
    if (!timeRangeParameters) {
      return Stream.of(new InputSplit<>(Intervals.ETERNITY));
    }

    final Interval bounds;
    try (final RemoteDruidMetadataClient client = new RemoteDruidMetadataClient(connection, mapper)) {
      bounds = client.discoverTimeBounds(dataSource);
    }
    if (bounds == null) {
      return Stream.empty();
    }
    final List<Interval> ranges = new ArrayList<>();
    for (final Interval interval : JodaUtils.condenseIntervals(intervals == null ? List.of(bounds) : intervals)) {
      if (!interval.overlaps(bounds)) {
        continue;
      }
      final Interval clipped = interval.overlap(bounds);
      if (splitDuration == null) {
        ranges.add(clipped);
      } else {
        for (long start = clipped.getStartMillis(); start < clipped.getEndMillis(); ) {
          final long end = Math.min(clipped.getEndMillis(), DateTimes.utc(start).plus(splitDuration).getMillis());
          ranges.add(Intervals.utc(start, end));
          start = end;
          if (ranges.size() > MAX_SPLITS) {
            throw new IOException("Remote Druid read would exceed " + MAX_SPLITS + " splits; use a longer splitDuration");
          }
        }
      }
    }
    return ranges.stream().map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(@Nullable final InputFormat inputFormat, @Nullable final SplitHintSpec splitHintSpec)
  {
    return 1;
  }

  @Override
  public RemoteDruidSqlInputSource withSplit(final InputSplit<Interval> split)
  {
    return new RemoteDruidSqlInputSource(
        connection,
        dataSource,
        sql,
        engine,
        context,
        signature,
        timestampColumns,
        timeRangeParameters,
        intervals,
        splitDuration,
        Preconditions.checkNotNull(split.get(), "split"),
        config,
        mapper
    );
  }

  @Override
  protected InputSourceReader fixedFormatReader(final InputRowSchema schema, @Nullable final File temporaryDirectory)
  {
    Preconditions.checkState(
        !timeRangeParameters || range != null,
        "A time-range remote Druid read must be bound to a split"
    );
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read(@Nullable final InputStats inputStats)
      {
        final ResultIterator results = new ResultIterator(inputStats);
        return new CloseableIterator<>()
        {
          @Override
          public boolean hasNext()
          {
            return results.hasNext();
          }

          @Override
          public InputRow next()
          {
            return MapInputRowParser.parse(schema, results.next());
          }

          @Override
          public void close()
          {
            results.close();
          }
        };
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample()
      {
        final ResultIterator results = new ResultIterator(null);
        return new CloseableIterator<>()
        {
          @Override
          public boolean hasNext()
          {
            return results.hasNext();
          }

          @Override
          public InputRowListPlusRawValues next()
          {
            final Map<String, Object> event = results.next();
            return InputRowListPlusRawValues.of(MapInputRowParser.parse(schema, event), event);
          }

          @Override
          public void close()
          {
            results.close();
          }
        };
      }
    };
  }

  /// Builds the source SQL API request, including the bound time range when the SQL is parameterized.
  Map<String, Object> makeRequest(final String sqlQueryId)
  {
    final Map<String, Object> requestContext = new HashMap<>(context);
    requestContext.put("engine", engine);
    requestContext.put("sqlQueryId", sqlQueryId);
    // Arrays must arrive as JSON arrays, not as JSON-encoded strings.
    requestContext.put("sqlStringifyArrays", false);

    final Map<String, Object> request = new LinkedHashMap<>();
    request.put("query", sql);
    request.put("resultFormat", "arrayLines");
    request.put("header", true);
    request.put("context", requestContext);
    if (timeRangeParameters) {
      request.put("parameters", List.of(
          Map.of("type", "BIGINT", "value", range.getStartMillis()),
          Map.of("type", "BIGINT", "value", range.getEndMillis())
      ));
    }
    return request;
  }

  /// Streams one source result. Transient failures are retried only before the first row is returned, since rows
  /// already handed to the caller cannot be taken back.
  private class ResultIterator implements CloseableIterator<Map<String, Object>>
  {
    @Nullable
    private final InputStats inputStats;
    private final RemoteDruidHttpClient httpClient;
    @Nullable
    private RemoteDruidHttpClient.Response response;
    @Nullable
    private InputStream body;
    @Nullable
    private String sqlQueryId;
    private boolean sourceRunning;
    @Nullable
    private Map<String, Object> nextRow;
    private int attempts;
    private long rowsReturned;
    private boolean done;
    private boolean closed;

    ResultIterator(@Nullable final InputStats inputStats)
    {
      this.inputStats = inputStats;
      this.httpClient = connection.getAuthentication().createClient(connection.getConnectTimeout(), connection.getReadTimeout());
    }

    @Override
    public boolean hasNext()
    {
      if (closed || done) {
        return false;
      }
      if (nextRow != null) {
        return true;
      }
      while (true) {
        try {
          if (body == null) {
            open();
          }
          final byte[] line = readLine(body);
          if (line == null) {
            throw new IOException("Remote Druid result ended without its completion marker");
          }
          if (line.length == 0) {
            if (body.read() != -1) {
              throw new IOException("Remote Druid result has data after its completion marker");
            }
            done = true;
            closeResponse(true);
            return false;
          }
          if (inputStats != null) {
            inputStats.incrementProcessedBytes(line.length + 1);
          }
          nextRow = toRow(mapper.readValue(line, new TypeReference<List<Object>>() {}));
          return true;
        }
        catch (IOException e) {
          closeResponse(false);
          if (rowsReturned > 0 || !isRetryable(e) || attempts > connection.getMaxRetries()) {
            close();
            throw new UncheckedIOException(e);
          }
          LOG.noStackTrace().warn(e, "Retrying remote Druid read of datasource[%s], range[%s]", dataSource, range);
          backoff();
        }
      }
    }

    @Override
    public Map<String, Object> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Map<String, Object> row = nextRow;
      nextRow = null;
      rowsReturned++;
      return row;
    }

    @Override
    public void close()
    {
      if (!closed) {
        closed = true;
        closeResponse(done);
        httpClient.close();
      }
    }

    private void open() throws IOException
    {
      checkInterrupted();
      attempts++;
      sqlQueryId = UUID.randomUUID().toString();
      response = httpClient.execute(sqlEndpoint(), "POST", mapper.writeValueAsBytes(makeRequest(sqlQueryId)));
      body = new BufferedInputStream(response.body());
      if (response.status() != 200) {
        throw new StatusException(response.status(), readError(body));
      }
      sourceRunning = true;
      final byte[] header = readLine(body);
      if (header == null) {
        throw new IOException("Remote Druid result ended before its header");
      }
      final List<Object> columns = mapper.readValue(header, new TypeReference<>() {});
      if (columns.size() != signature.size()) {
        throw new NonRetryableException(
            "Remote Druid result has " + columns.size() + " columns, but " + signature.size() + " were planned"
        );
      }
    }

    private Map<String, Object> toRow(final List<Object> values) throws IOException
    {
      if (values.size() != signature.size()) {
        throw new IOException("Remote Druid result row has an unexpected number of columns");
      }
      final Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 0; i < values.size(); i++) {
        final String column = signature.getColumnName(i);
        Object value = values.get(i);
        if (value instanceof String && timestampColumns.contains(column)) {
          value = DateTimes.of((String) value).getMillis();
        } else if (value instanceof Boolean) {
          value = (Boolean) value ? 1L : 0L;
        }
        row.put(column, value);
      }
      return row;
    }

    private void closeResponse(final boolean complete)
    {
      if (response != null) {
        response.close();
        response = null;
        body = null;
        if (!complete && sourceRunning) {
          cancel(sqlQueryId);
        }
        sourceRunning = false;
      }
    }

    private void cancel(final String queryId)
    {
      try (final RemoteDruidHttpClient.Response ignored = httpClient.execute(
          URI.create(sqlEndpoint() + "/" + StringUtils.urlEncode(queryId)), "DELETE", null
      )) {
        // Best effort; closing the connection also stops the source from streaming more results.
      }
      catch (Exception e) {
        LOG.debug("Unable to cancel remote Druid SQL query[%s]", queryId);
      }
    }

    private void backoff()
    {
      try {
        Thread.sleep(Math.min(10_000L, 250L << Math.min(attempts, 5)));
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        close();
        throw new UncheckedIOException(new InterruptedIOException("Remote Druid read interrupted"));
      }
    }
  }

  private URI sqlEndpoint()
  {
    return URI.create(connection.getEndpoint() + "/sql");
  }

  private static boolean isRetryable(final IOException e)
  {
    if (e instanceof NonRetryableException || e instanceof InterruptedIOException) {
      return false;
    }
    if (e instanceof StatusException) {
      final int status = ((StatusException) e).status;
      return status == 408 || status == 429 || status >= 500;
    }
    return !Thread.currentThread().isInterrupted();
  }

  /// Reads one newline-terminated line without the terminator, or returns null at end of stream.
  @Nullable
  static byte[] readLine(final InputStream in) throws IOException
  {
    final ByteArrayOutputStream line = new ByteArrayOutputStream();
    int b;
    while ((b = in.read()) != '\n') {
      if (b == -1) {
        if (line.size() == 0) {
          return null;
        }
        throw new IOException("Remote Druid result ended in the middle of a row");
      }
      if (line.size() >= MAX_LINE_BYTES) {
        throw new NonRetryableException("Remote Druid result row exceeds " + MAX_LINE_BYTES + " bytes");
      }
      line.write(b);
    }
    checkInterrupted();
    return line.toByteArray();
  }

  private String readError(final InputStream in)
  {
    try {
      final byte[] bytes = in.readNBytes(MAX_ERROR_BYTES);
      final Map<String, Object> error = mapper.readValue(bytes, new TypeReference<>() {});
      final Object message = error.getOrDefault("errorMessage", error.get("error"));
      return message == null ? "unknown error" : StringUtils.chop(String.valueOf(message), 1024);
    }
    catch (Exception e) {
      return "unknown error";
    }
  }

  private static void checkInterrupted() throws InterruptedIOException
  {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Remote Druid read interrupted");
    }
  }

  @Override
  public boolean equals(final Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RemoteDruidSqlInputSource that)) {
      return false;
    }
    return timeRangeParameters == that.timeRangeParameters
           && connection.equals(that.connection)
           && dataSource.equals(that.dataSource)
           && sql.equals(that.sql)
           && engine.equals(that.engine)
           && context.equals(that.context)
           && signature.equals(that.signature)
           && timestampColumns.equals(that.timestampColumns)
           && Objects.equals(intervals, that.intervals)
           && Objects.equals(splitDuration, that.splitDuration)
           && Objects.equals(range, that.range);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        connection,
        dataSource,
        sql,
        engine,
        context,
        signature,
        timestampColumns,
        timeRangeParameters,
        intervals,
        splitDuration,
        range
    );
  }

  @Override
  public String toString()
  {
    return "RemoteDruidSqlInputSource{endpoint=" + connection.getEndpoint()
           + ", engine=" + engine
           + ", sql=" + sql
           + ", range=" + range
           + '}';
  }

  private static class StatusException extends IOException
  {
    private final int status;

    StatusException(final int status, final String message)
    {
      super("Remote Druid SQL request failed with HTTP status[" + status + "]: " + message);
      this.status = status;
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
