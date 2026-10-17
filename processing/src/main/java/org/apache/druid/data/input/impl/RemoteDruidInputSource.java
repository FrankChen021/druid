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
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/// Describes a remote Druid table referenced by the `DRUID` table function. SQL planning replaces each scan of it with
/// a [RemoteDruidSqlInputSource] that runs the pushed-down SELECT on the source cluster; it is never read directly.
public class RemoteDruidInputSource extends AbstractInputSource
{
  public static final String TYPE_KEY = "remoteDruid";
  private final RemoteDruidConnection connection;
  private final String dataSource;
  @Nullable
  private final List<Interval> intervals;
  private final String engine;
  @Nullable
  private final Period splitDuration;
  @Nullable
  private final String includeSegmentSource;
  private final HttpInputSourceConfig config;
  private final ObjectMapper mapper;

  @JsonCreator
  public RemoteDruidInputSource(
      @JsonProperty("connection") final RemoteDruidConnection connection,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("intervals") @Nullable final List<Interval> intervals,
      @JsonProperty("engine") @Nullable final String engine,
      @JsonProperty("splitDuration") @Nullable final Period splitDuration,
      @JsonProperty("includeSegmentSource") @Nullable final String includeSegmentSource,
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
    this.engine = engine == null ? RemoteDruidSqlInputSource.ENGINE_DART : engine;
    this.splitDuration = splitDuration;
    // Dart reads only published segments by default, so that every time range reads the same set of segments.
    this.includeSegmentSource = includeSegmentSource == null && RemoteDruidSqlInputSource.ENGINE_DART.equals(this.engine)
                                ? RemoteDruidSqlInputSource.SEGMENT_SOURCE_NONE
                                : includeSegmentSource;
    Preconditions.checkArgument(
        includeSegmentSource == null || RemoteDruidSqlInputSource.ENGINE_DART.equals(this.engine),
        "includeSegmentSource applies only to the [%s] engine; the native engine always reads realtime segments",
        RemoteDruidSqlInputSource.ENGINE_DART
    );
    Preconditions.checkArgument(
        this.includeSegmentSource == null
        || RemoteDruidSqlInputSource.SEGMENT_SOURCES.contains(this.includeSegmentSource),
        "includeSegmentSource must be one of %s",
        RemoteDruidSqlInputSource.SEGMENT_SOURCES
    );
    Preconditions.checkArgument(
        RemoteDruidSqlInputSource.ENGINES.contains(this.engine),
        "Remote Druid engine must be one of %s",
        RemoteDruidSqlInputSource.ENGINES
    );
    Preconditions.checkArgument(
        splitDuration == null || RemoteDruidSqlInputSource.isPositivePeriod(splitDuration),
        "splitDuration must be a positive period without negative fields"
    );
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

  @JsonIgnore
  @Override
  public Set<String> getTypes()
  {
    return Set.of(TYPE_KEY);
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  /// Discovers primitive column types for SQL planning when no explicit EXTEND schema is provided.
  public RowSignature discoverSchema() throws IOException
  {
    try (final RemoteDruidMetadataClient client = new RemoteDruidMetadataClient(connection, mapper)) {
      return client.discoverSchema(dataSource);
    }
  }

  /// Creates the input source that streams one pushed-down source SELECT through this connection.
  ///
  /// @param sql                 source SQL; when `timeRangeParameters` is set, it filters `__time` on two
  ///                            `BIGINT` epoch-millisecond parameters, `[start, end)`
  /// @param signature           output columns, in the order of the SQL result
  /// @param timestampColumns    output columns that the source returns as SQL timestamps
  /// @param context             source query context
  public RemoteDruidSqlInputSource toSqlInputSource(
      final String sql,
      final boolean timeRangeParameters,
      final RowSignature signature,
      final List<String> timestampColumns,
      final Map<String, Object> context
  )
  {
    final Map<String, Object> sourceContext = new HashMap<>(context);
    if (includeSegmentSource != null) {
      sourceContext.put(RemoteDruidSqlInputSource.CTX_INCLUDE_SEGMENT_SOURCE, includeSegmentSource);
    }
    return new RemoteDruidSqlInputSource(
        connection,
        dataSource,
        sql,
        engine,
        sourceContext,
        signature,
        timestampColumns,
        timeRangeParameters,
        timeRangeParameters ? intervals : null,
        timeRangeParameters ? splitDuration : null,
        null,
        config,
        mapper
    );
  }

  /// Requires SQL planning to create the source SELECT before an ingestion worker can read the remote table.
  @Override
  protected InputSourceReader fixedFormatReader(final InputRowSchema schema, @Nullable final File temporaryDirectory)
  {
    throw new UOE("Remote Druid tables must be read through SQL using TABLE(DRUID(...))");
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
    return connection.equals(that.connection) && dataSource.equals(that.dataSource)
           && Objects.equals(intervals, that.intervals) && engine.equals(that.engine)
           && Objects.equals(splitDuration, that.splitDuration)
           && Objects.equals(includeSegmentSource, that.includeSegmentSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connection, dataSource, intervals, engine, splitDuration, includeSegmentSource);
  }

  @Override
  public String toString()
  {
    return "RemoteDruidInputSource{endpoint=" + connection.getEndpoint() + ", dataSource=" + dataSource + ", intervals=" + intervals + '}';
  }
}
