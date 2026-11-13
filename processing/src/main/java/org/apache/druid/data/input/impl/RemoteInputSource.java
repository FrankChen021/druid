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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTimeZone;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A table on a remote Druid cluster, read through that cluster's SQL API.
 *
 * The {@code REMOTE} table function creates it without {@link #getSql()}, as can {@code EXTERN} specs: a reference to
 * the remote table that can only be planned, not read. SQL planning pushes the operators above the table into a source
 * SELECT and returns a copy of this input source with that SQL, which ingestion reads: the source runs it with its own
 * engine (Dart by default) and streams {@code arrayLines} results, so the source cluster needs neither tasks nor
 * durable storage. An {@code EXTERN} spec that already has {@link #getSql()} is read as it is. When {@link
 * #isTimeRangeParameters()} is true, the SQL has two {@code BIGINT} parameters, {@code [start, end)} in epoch
 * milliseconds, and each split reads one time range.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RemoteInputSource extends AbstractInputSource implements SplittableInputSource<Interval>
{
  public static final String TYPE_KEY = "remote";
  private static final int CANCEL_TIMEOUT_MILLIS = 10_000;
  public static final String ENGINE_DART = "msq-dart";
  public static final String ENGINE_NATIVE = "native";
  public static final Set<String> ENGINES = Set.of(ENGINE_DART, ENGINE_NATIVE);
  /**
   * Dart query context key choosing whether realtime segments are read, in addition to published ones.
   */
  public static final String CTX_INCLUDE_SEGMENT_SOURCE = "includeSegmentSource";
  public static final String SEGMENT_SOURCE_NONE = "NONE";
  public static final String SEGMENT_SOURCE_REALTIME = "REALTIME";
  public static final Set<String> SEGMENT_SOURCES = Set.of(SEGMENT_SOURCE_NONE, SEGMENT_SOURCE_REALTIME);
  static final int MAX_SPLITS = 10_000;
  static final int MAX_LINE_BYTES = 16 * 1024 * 1024;
  private static final Logger LOG = new Logger(RemoteInputSource.class);

  /**
   * Base delay before the first retry, doubled for each later one. Tests set it to zero.
   */
  @VisibleForTesting
  static volatile long retryBackoffMillis = 250;

  private final RemoteConnection connection;
  private final String dataSource;
  @Nullable
  private final List<Interval> intervals;
  private final String engine;
  @Nullable
  private final Period splitDuration;
  @Nullable
  private final String includeSegmentSource;
  private final boolean declaredSchema;
  @Nullable
  private final String sql;
  private final Map<String, Object> context;
  @Nullable
  private final RowSignature signature;
  private final List<String> timestampColumns;
  private final boolean timeRangeParameters;
  @Nullable
  private final Interval range;
  private final HttpInputSourceConfig config;
  private final ObjectMapper mapper;

  @JsonCreator
  public RemoteInputSource(
      @JsonProperty("connection") final RemoteConnection connection,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("intervals") @Nullable final List<Interval> intervals,
      @JsonProperty("engine") @Nullable final String engine,
      @JsonProperty("splitDuration") @Nullable final Period splitDuration,
      @JsonProperty("includeSegmentSource") @Nullable final String includeSegmentSource,
      @JsonProperty("declaredSchema") @Nullable final Boolean declaredSchema,
      @JsonProperty("sql") @Nullable final String sql,
      @JsonProperty("context") @Nullable final Map<String, Object> context,
      @JsonProperty("signature") @Nullable final RowSignature signature,
      @JsonProperty("timestampColumns") @Nullable final List<String> timestampColumns,
      @JsonProperty("timeRangeParameters") @Nullable final Boolean timeRangeParameters,
      @JsonProperty("range") @Nullable final Interval range,
      @JacksonInject final HttpInputSourceConfig config,
      @JacksonInject @Json final ObjectMapper mapper
  )
  {
    this.connection = Preconditions.checkNotNull(connection, "connection");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.intervals = intervals == null
                     ? null
                     : List.copyOf(JodaUtils.condenseIntervals(
                         intervals.stream().filter(i -> i.toDurationMillis() > 0).toList()
                     ));
    this.engine = engine == null ? ENGINE_DART : engine;
    this.splitDuration = splitDuration;
    // Dart reads only published segments by default, so that every time range reads the same set of segments.
    this.includeSegmentSource = includeSegmentSource == null && ENGINE_DART.equals(this.engine)
                                ? SEGMENT_SOURCE_NONE
                                : includeSegmentSource;
    // EXTERN specs always give a signature, so a missing value means declared.
    this.declaredSchema = declaredSchema == null || declaredSchema;
    this.sql = sql;
    // Sorted, so that serialized specs and EXPLAIN output are deterministic.
    this.context = context == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(context));
    this.signature = signature;
    this.timestampColumns = timestampColumns == null ? List.of() : List.copyOf(timestampColumns);
    this.timeRangeParameters = timeRangeParameters != null && timeRangeParameters;
    this.range = range;
    this.config = Preconditions.checkNotNull(config, "config");
    this.mapper = Preconditions.checkNotNull(mapper, "mapper");

    Preconditions.checkArgument(ENGINES.contains(this.engine), "Remote engine must be one of %s", ENGINES);
    Preconditions.checkArgument(
        includeSegmentSource == null || ENGINE_DART.equals(this.engine),
        "includeSegmentSource applies only to the [%s] engine; the native engine always reads realtime segments",
        ENGINE_DART
    );
    Preconditions.checkArgument(
        this.includeSegmentSource == null || SEGMENT_SOURCES.contains(this.includeSegmentSource),
        "includeSegmentSource must be one of %s",
        SEGMENT_SOURCES
    );
    Preconditions.checkArgument(
        splitDuration == null || sql == null || this.timeRangeParameters,
        "splitDuration applies only to a remote read with sql that has time-range parameters"
    );
    Preconditions.checkArgument(
        splitDuration == null || isPositivePeriod(splitDuration),
        "splitDuration must be a positive period without negative fields"
    );
    Preconditions.checkArgument(sql == null || signature != null, "A remote read with sql requires a signature");
    Preconditions.checkArgument(
        sql != null || (signature == null && !this.timeRangeParameters && range == null),
        "Only a remote read with sql has a signature, time-range parameters, or a range"
    );
    Preconditions.checkArgument(
        Set.of("http", "https").contains(StringUtils.toLowerCase(connection.getEndpoint().getScheme())),
        "Remote Druid supports HTTP(S) endpoints"
    );
    HttpInputSource.throwIfInvalidProtocols(config, List.of(connection.getEndpoint()));
  }

  /**
   * Creates a reference to a remote table, for SQL planning to read through {@link #withPlannedQuery}.
   */
  public static RemoteInputSource table(
      final RemoteConnection connection,
      final String dataSource,
      @Nullable final List<Interval> intervals,
      @Nullable final String engine,
      @Nullable final Period splitDuration,
      @Nullable final String includeSegmentSource,
      final boolean declaredSchema,
      final HttpInputSourceConfig config,
      final ObjectMapper mapper
  )
  {
    return new RemoteInputSource(
        connection,
        dataSource,
        intervals,
        engine,
        splitDuration,
        includeSegmentSource,
        declaredSchema,
        null,
        null,
        null,
        null,
        null,
        null,
        config,
        mapper
    );
  }

  /**
   * Returns a copy that reads the given source SQL.
   *
   * @param sql                 source SQL; when {@code timeRangeParameters} is set, it filters {@code __time} on two
   *                            {@code BIGINT} epoch-millisecond parameters, {@code [start, end)}
   * @param signature           output columns, in the order of the SQL result
   * @param timestampColumns    output columns that the source returns as SQL timestamps
   * @param context             source query context
   * @param filteredTimeRange   {@code __time} range outside which the SQL matches no rows, or null if unbounded; reads
   *                            split by time are only split over this range
   */
  public RemoteInputSource withPlannedQuery(
      final String sql,
      final boolean timeRangeParameters,
      final RowSignature signature,
      final List<String> timestampColumns,
      final Map<String, Object> context,
      @Nullable final Interval filteredTimeRange
  )
  {
    return new RemoteInputSource(
        connection,
        dataSource,
        restrictIntervals(intervals, filteredTimeRange),
        engine,
        // Only reads that are split by time use the split duration.
        timeRangeParameters ? splitDuration : null,
        includeSegmentSource,
        declaredSchema,
        Preconditions.checkNotNull(sql, "sql"),
        context,
        signature,
        timestampColumns,
        timeRangeParameters,
        null,
        config,
        mapper
    );
  }

  @JsonProperty
  public RemoteConnection getConnection()
  {
    return connection;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Nullable
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public String getEngine()
  {
    return engine;
  }

  @JsonProperty
  @Nullable
  public Period getSplitDuration()
  {
    return splitDuration;
  }

  @JsonProperty
  @Nullable
  public String getIncludeSegmentSource()
  {
    return includeSegmentSource;
  }

  /**
   * Whether column types come from {@code EXTEND} or {@code EXTERN} rather than from the source, so they may differ
   * from it.
   */
  @JsonProperty
  public boolean isDeclaredSchema()
  {
    return declaredSchema;
  }

  /**
   * The source SELECT that this input source reads; null for a remote table that is not planned yet.
   */
  @JsonProperty
  @Nullable
  public String getSql()
  {
    return sql;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getContext()
  {
    return context;
  }

  @JsonProperty
  @Nullable
  public RowSignature getSignature()
  {
    return signature;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getTimestampColumns()
  {
    return timestampColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isTimeRangeParameters()
  {
    return timeRangeParameters;
  }

  /**
   * The time range read by a bound split; null before splitting.
   */
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
    return Set.of(TYPE_KEY);
  }

  /**
   * Discovers primitive column types for SQL planning when no explicit EXTEND schema is provided.
   */
  public RowSignature discoverSchema() throws IOException
  {
    try (final RemoteMetadataClient client = new RemoteMetadataClient(connection, mapper)) {
      return client.discoverSchema(dataSource);
    }
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
    checkPlanned();
    if (range != null) {
      return Stream.of(new InputSplit<>(range));
    }
    if (!timeRangeParameters) {
      return Stream.of(new InputSplit<>(Intervals.ETERNITY));
    }

    final Interval bounds;
    try (final RemoteMetadataClient client = new RemoteMetadataClient(connection, mapper)) {
      bounds = client.discoverTimeBounds(dataSource);
    }
    catch (RemoteStatusException e) {
      if (e.isUserError()) {
        throw InvalidInput.exception(e, "Remote Druid read failed: %s", e.getMessage());
      }
      throw e;
    }
    final List<Interval> requested = intervals == null ? null : JodaUtils.condenseIntervals(intervals);
    if (bounds == null) {
      // An empty result also comes from a missing datasource or a Broker without a segment view. Read the requested
      // ranges anyway, so the source validates the table rather than an ingestion succeeding, possibly replacing
      // data, with no rows.
      return (requested == null ? List.of(Intervals.ETERNITY) : requested).stream().map(InputSplit::new);
    }
    final List<Interval> ranges = new ArrayList<>();
    for (final Interval interval : requested == null ? List.of(bounds) : requested) {
      if (!interval.overlaps(bounds)) {
        continue;
      }
      final Interval clipped = interval.overlap(bounds);
      if (splitDuration == null) {
        ranges.add(clipped);
      } else {
        // Align ranges to UTC period boundaries, so that they line up with time partitioning on the target.
        final Granularity granularity = new PeriodGranularity(splitDuration, null, DateTimeZone.UTC);
        for (final Interval bucket : granularity.getIterable(clipped)) {
          ranges.add(bucket.overlap(clipped));
          if (ranges.size() > MAX_SPLITS) {
            throw new IOException(
                "Remote Druid read would exceed " + MAX_SPLITS + " splits; use a longer splitDuration"
            );
          }
        }
      }
    }
    return ranges.stream().map(InputSplit::new);
  }

  /**
   * Reports a read failure that the query must change to fix, such as rejected credentials or invalid source SQL, as
   * invalid input, and other failures as I/O errors.
   */
  private static RuntimeException readFailure(final IOException e)
  {
    if (e instanceof RemoteSchemaException || (e instanceof RemoteStatusException status && status.isUserError())) {
      return InvalidInput.exception(e, "Remote Druid read failed: %s", e.getMessage());
    }
    return new UncheckedIOException(e);
  }

  /**
   * Intersects the requested intervals with a range outside which no rows match.
   */
  @Nullable
  private static List<Interval> restrictIntervals(
      @Nullable final List<Interval> intervals,
      @Nullable final Interval range
  )
  {
    if (range == null) {
      return intervals;
    }
    if (intervals == null) {
      return List.of(range);
    }
    return intervals.stream().filter(range::overlaps).map(range::overlap).toList();
  }

  @Override
  public int estimateNumSplits(@Nullable final InputFormat inputFormat, @Nullable final SplitHintSpec splitHintSpec)
  {
    return 1;
  }

  @Override
  public RemoteInputSource withSplit(final InputSplit<Interval> split)
  {
    checkPlanned();
    return new RemoteInputSource(
        connection,
        dataSource,
        intervals,
        engine,
        splitDuration,
        includeSegmentSource,
        declaredSchema,
        sql,
        context,
        signature,
        timestampColumns,
        timeRangeParameters,
        Preconditions.checkNotNull(split.get(), "split"),
        config,
        mapper
    );
  }

  @Override
  protected InputSourceReader fixedFormatReader(final InputRowSchema schema, @Nullable final File temporaryDirectory)
  {
    checkPlanned();
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

  /**
   * Builds the source SQL API request, including the bound time range when the SQL is parameterized.
   */
  Map<String, Object> makeRequest(final String sqlQueryId)
  {
    final Map<String, Object> requestContext = new HashMap<>(context);
    requestContext.put(QueryContexts.ENGINE, engine);
    requestContext.put(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId);
    // Arrays must arrive as JSON arrays, not as JSON-encoded strings.
    requestContext.put(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false);
    if (includeSegmentSource != null) {
      requestContext.put(CTX_INCLUDE_SEGMENT_SOURCE, includeSegmentSource);
    }

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

  /**
   * Streams one source result. Transient failures are retried only before the first row is returned, since rows
   * already handed to the caller cannot be taken back.
   */
  private class ResultIterator implements CloseableIterator<Map<String, Object>>
  {
    @Nullable
    private final InputStats inputStats;
    private final RemoteHttpClient httpClient;
    @Nullable
    private RemoteHttpClient.Response response;
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
      this.httpClient = connection.createClient(connection.getReadTimeout());
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
            throw readFailure(e);
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
      final URI endpoint = sqlEndpoint();
      response = httpClient.execute(endpoint, "POST", mapper.writeValueAsBytes(makeRequest(sqlQueryId)));
      body = new BufferedInputStream(response.body());
      if (response.status() != 200) {
        throw new RemoteStatusException(endpoint, response.status(), RemoteStatusException.readMessage(body, mapper));
      }
      sourceRunning = true;
      final byte[] header = readLine(body);
      if (header == null) {
        throw new IOException("Remote Druid result ended before its header");
      }
      final List<Object> columns = mapper.readValue(header, new TypeReference<>() {});
      // Rows are mapped by position, so the source must return the planned columns in the planned order.
      if (columns == null || !signature.getColumnNames().equals(columns)) {
        throw new NonRetryableException(
            "Remote Druid result has columns " + columns + ", but " + signature.getColumnNames() + " were planned"
        );
      }
    }

    private Map<String, Object> toRow(@Nullable final List<Object> values) throws IOException
    {
      if (values == null || values.size() != signature.size()) {
        throw new NonRetryableException("Remote Druid result row has an unexpected number of columns");
      }
      final Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 0; i < values.size(); i++) {
        final String column = signature.getColumnName(i);
        Object value = values.get(i);
        if (value instanceof String && timestampColumns.contains(column)) {
          try {
            value = DateTimes.of((String) value).getMillis();
          }
          catch (IllegalArgumentException e) {
            throw new NonRetryableException("Remote Druid result has an invalid timestamp in column[" + column + "]");
          }
        } else if (value instanceof Boolean) {
          value = (Boolean) value ? 1L : 0L;
        } else if (value instanceof List && isStringColumn(column)) {
          // String columns are requested as MV_TO_ARRAY, so that multi-value rows arrive as arrays.
          final List<?> elements = (List<?>) value;
          value = elements.isEmpty() ? null : elements.size() == 1 ? elements.get(0) : elements;
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
      // Abandoned reads are often interrupted. Clear the flag while cancelling, so the request is not abandoned too.
      final boolean interrupted = Thread.interrupted();
      // A separate client with a short timeout, so that a hung source does not hold up the abandoned read.
      try (final RemoteHttpClient cancelClient = connection.createClient(
               Math.min(connection.getReadTimeout(), CANCEL_TIMEOUT_MILLIS)
           );
           final RemoteHttpClient.Response ignored = cancelClient.execute(
               URI.create(sqlEndpoint() + "/" + StringUtils.urlEncode(queryId)), "DELETE", null
           )) {
        // Best effort; closing the connection also stops the source from streaming more results.
      }
      catch (Exception e) {
        LOG.debug("Unable to cancel remote Druid SQL query[%s]", queryId);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private void backoff()
    {
      try {
        Thread.sleep(Math.min(10_000L, retryBackoffMillis << Math.min(attempts, 5)));
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

  private void checkPlanned()
  {
    if (sql == null) {
      throw new UOE("Remote tables must be read through SQL planning, such as with TABLE(REMOTE(...))");
    }
  }

  private boolean isStringColumn(final String column)
  {
    return ColumnType.STRING.equals(signature.getColumnType(column).orElse(null));
  }

  /**
   * Whether a period moves time forward from every instant: no field is negative and at least one is positive.
   */
  static boolean isPositivePeriod(final Period period)
  {
    boolean positive = false;
    for (final int value : period.getValues()) {
      if (value < 0) {
        return false;
      }
      positive |= value > 0;
    }
    return positive;
  }

  private static boolean isRetryable(final IOException e)
  {
    if (e instanceof NonRetryableException
        || e instanceof RemoteHttpClient.ConfigurationException
        || e instanceof JsonProcessingException
        || e instanceof InterruptedIOException) {
      return false;
    }
    if (e instanceof RemoteStatusException status) {
      return status.isRetryable();
    }
    return !Thread.currentThread().isInterrupted();
  }

  /**
   * Reads one newline-terminated line without the terminator, or returns null at end of stream.
   */
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
    if (!(other instanceof RemoteInputSource that)) {
      return false;
    }
    return timeRangeParameters == that.timeRangeParameters
           && declaredSchema == that.declaredSchema
           && connection.equals(that.connection)
           && dataSource.equals(that.dataSource)
           && engine.equals(that.engine)
           && Objects.equals(intervals, that.intervals)
           && Objects.equals(splitDuration, that.splitDuration)
           && Objects.equals(includeSegmentSource, that.includeSegmentSource)
           && Objects.equals(sql, that.sql)
           && context.equals(that.context)
           && Objects.equals(signature, that.signature)
           && timestampColumns.equals(that.timestampColumns)
           && Objects.equals(range, that.range);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        connection,
        dataSource,
        intervals,
        engine,
        splitDuration,
        includeSegmentSource,
        declaredSchema,
        sql,
        context,
        signature,
        timestampColumns,
        timeRangeParameters,
        range
    );
  }

  @Override
  public String toString()
  {
    return "RemoteInputSource{endpoint=" + connection.getEndpoint()
           + ", dataSource=" + dataSource
           + ", engine=" + engine
           + ", sql=" + sql
           + ", range=" + range
           + '}';
  }

  private static class NonRetryableException extends IOException
  {
    NonRetryableException(final String message)
    {
      super(message);
    }
  }
}
