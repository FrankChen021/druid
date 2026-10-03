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
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/** Reads stored rows through a remote cluster's native query endpoint. */
public class RemoteDruidInputSource extends AbstractInputSource implements SplittableInputSource<Interval>
{
  public static final String TYPE_KEY = "remoteDruid";
  private static final long DEFAULT_SPLIT_DURATION = 3_600_000;
  private static final int MAX_SPLITS = 10_000;
  private final RemoteDruidConnection connection;
  private final String dataSource;
  @Nullable
  private final List<Interval> intervals;
  private final long splitDurationMillis;
  @Nullable
  private final DimFilter filter;
  private final boolean split;
  private final HttpInputSourceConfig config;
  private final ObjectMapper mapper;

  @JsonCreator
  public RemoteDruidInputSource(
      @JsonProperty("connection") final RemoteDruidConnection connection,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("intervals") @Nullable final List<Interval> intervals,
      @JsonProperty("splitDurationMillis") @Nullable final Long splitDurationMillis,
      @JsonProperty("filter") @Nullable final DimFilter filter,
      @JsonProperty("split") final boolean split,
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
    this.splitDurationMillis = splitDurationMillis == null ? DEFAULT_SPLIT_DURATION : splitDurationMillis;
    Preconditions.checkArgument(this.splitDurationMillis > 0, "splitDurationMillis must be positive");
    Preconditions.checkArgument(!split || (this.intervals != null && this.intervals.size() == 1), "A split needs one interval");
    this.filter = filter;
    this.split = split;
    this.config = Preconditions.checkNotNull(config, "config");
    this.mapper = Preconditions.checkNotNull(mapper, "mapper");
    Preconditions.checkArgument(Set.of("http", "https").contains(StringUtils.toLowerCase(connection.getEndpoint().getScheme())),
                                "Remote Druid supports HTTP(S) endpoints");
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
  @Nullable
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public long getSplitDurationMillis()
  {
    return splitDurationMillis;
  }

  @JsonProperty
  @Nullable
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public boolean isSplit()
  {
    return split;
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

  public RemoteDruidInputSource withReadFilter(@Nullable final DimFilter readFilter, final List<Interval> readIntervals)
  {
    final DimFilter combined = filter == null ? readFilter
                                             : readFilter == null ? filter : new AndDimFilter(List.of(filter, readFilter));
    return new RemoteDruidInputSource(
        connection,
        dataSource,
        intersect(intervals, readIntervals),
        splitDurationMillis,
        combined,
        split,
        config,
        mapper
    );
  }

  private static List<Interval> intersect(@Nullable final List<Interval> left, final List<Interval> right)
  {
    if (left == null) {
      return right;
    }
    final List<Interval> result = new ArrayList<>();
    for (final Interval a : left) {
      for (final Interval b : right) {
        final Interval overlap = a.overlap(b);
        if (overlap != null) {
          result.add(overlap);
        }
      }
    }
    return result;
  }

  @Override
  public Stream<InputSplit<Interval>> createSplits(@Nullable final InputFormat inputFormat, @Nullable final SplitHintSpec hint)
      throws IOException
  {
    if (intervals != null && intervals.isEmpty()) {
      return Stream.empty();
    }
    if (split) {
      return intervals.stream().map(InputSplit::new);
    }
    final Interval extent;
    try (final RemoteDruidInputSourceClient client = client()) {
      extent = client.timeBoundary(dataSource);
    }
    if (extent == null) {
      return Stream.empty();
    }
    final List<Interval> resolved = intersect(intervals, List.of(extent));
    long count = 0;
    for (final Interval interval : resolved) {
      count = Math.addExact(count, 1 + (interval.toDurationMillis() - 1) / splitDurationMillis);
      Preconditions.checkArgument(count <= MAX_SPLITS, "Too many remote splits; increase splitDurationMillis");
    }
    return resolved.stream().flatMap(interval -> {
      final long numSplits = 1 + (interval.toDurationMillis() - 1) / splitDurationMillis;
      return LongStream.range(0, numSplits).mapToObj(i -> {
        final long start = interval.getStartMillis() + i * splitDurationMillis;
        final long end = start + Math.min(splitDurationMillis, interval.getEndMillis() - start);
        return new InputSplit<>(new Interval(start, end, interval.getChronology()));
      });
    });
  }

  @Override
  public int estimateNumSplits(@Nullable final InputFormat format, @Nullable final SplitHintSpec hint) throws IOException
  {
    try (final Stream<InputSplit<Interval>> splits = createSplits(format, hint)) {
      return Math.toIntExact(splits.count());
    }
  }

  @Override
  public RemoteDruidInputSource withSplit(final InputSplit<Interval> selected)
  {
    return new RemoteDruidInputSource(
        connection,
        dataSource,
        List.of(selected.get()),
        splitDurationMillis,
        filter,
        true,
        config,
        mapper
    );
  }

  public RowSignature discoverSchema() throws IOException
  {
    try (final RemoteDruidInputSourceClient client = client()) {
      return client.discoverSchema(dataSource);
    }
  }

  private RemoteDruidInputSourceClient client()
  {
    return new RemoteDruidInputSourceClient(connection, mapper);
  }

  @Override
  protected InputSourceReader fixedFormatReader(final InputRowSchema schema, @Nullable final File temporaryDirectory)
  {
    Preconditions.checkNotNull(temporaryDirectory, "temporaryDirectory");
    for (final DimensionSchema dimension : schema.getDimensionsSpec().getDimensions()) {
      final org.apache.druid.segment.column.ColumnType type = dimension.getColumnType();
      Preconditions.checkArgument(
          type != null && (type.isPrimitive() || type.isPrimitiveArray()),
          "Remote Druid input supports only primitive and primitive-array columns"
      );
    }
    final Set<String> names = new LinkedHashSet<>();
    names.add("__time");
    names.addAll(schema.getDimensionsSpec().getDimensionNames());
    if (schema.getMetricNames() != null) {
      names.addAll(schema.getMetricNames());
    }
    Preconditions.checkArgument("__time".equals(schema.getTimestampSpec().getTimestampColumn()), "Use __time as timestampColumn");
    final List<String> columns = List.copyOf(names);
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read(@Nullable final InputStats stats) throws IOException
      {
        return rows(stats);
      }

      private CloseableIterator<InputRow> rows(@Nullable final InputStats stats) throws IOException
      {
        final List<InputSplit<Interval>> work;
        try (final Stream<InputSplit<Interval>> splits = createSplits(null, null)) {
          work = splits.toList();
        }
        return CloseableIterators.withEmptyBaggage(work.iterator()).flatMap(selected -> {
          try {
            final File file;
            try (final RemoteDruidInputSourceClient client = client()) {
              file = client.scan(dataSource, selected.get(), columns, filter, temporaryDirectory);
            }
            try {
              final CloseableIterator<InputRow> rows = new JsonInputFormat(null, null, true, true, false)
                  .createReader(schema, new FileEntity(file), temporaryDirectory).read();
              if (stats != null) {
                stats.incrementProcessedBytes(file.length());
              }
              return CloseableIterators.wrap(rows, () -> {
                try {
                  rows.close();
                }
                finally {
                  Files.deleteIfExists(file.toPath());
                }
              });
            }
            catch (Exception e) {
              Files.deleteIfExists(file.toPath());
              throw e;
            }
          }
          catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
      {
        return rows(null).map(row -> {
          final Map<String, Object> raw = new LinkedHashMap<>();
          for (final String column : columns) {
            raw.put(column, row.getRaw(column));
          }
          return InputRowListPlusRawValues.of(row, raw);
        });
      }
    };
  }

  @Override
  public boolean equals(final Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RemoteDruidInputSource that)) {
      return false;
    }
    return split == that.split && splitDurationMillis == that.splitDurationMillis
           && connection.equals(that.connection) && dataSource.equals(that.dataSource)
           && Objects.equals(intervals, that.intervals) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connection, dataSource, intervals, splitDurationMillis, filter, split);
  }

  @Override
  public String toString()
  {
    return "RemoteDruidInputSource{endpoint=" + connection.getEndpoint() + ", dataSource=" + dataSource + ", intervals=" + intervals + '}';
  }
}
