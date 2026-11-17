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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Streams one source result. Transient failures are retried only before the first row is returned, since rows
 * already handed to the caller cannot be taken back.
 */
class RemoteResultIterator implements CloseableIterator<Map<String, Object>>
{
  private static final TypeReference<List<Object>> ROW_TYPE = new TypeReference<>()
  {
  };
  private static final Logger LOG = new Logger(RemoteResultIterator.class);

  /**
   * Base delay before the first retry, doubled for each later one. Tests set it to zero.
   */
  @VisibleForTesting
  static volatile long retryBackoffMillis = 250;

  private final RemoteInputSource source;
  private final RemoteConnection connection;
  private final ObjectMapper mapper;
  private final RowSignature signature;
  private final JsonFactory jsonFactory;

  // Per-column facts, by position in the signature, so that decoding a value needs no lookups.
  private final String[] columnNames;
  private final boolean[] stringColumns;
  private final boolean[] timestampColumns;
  @Nullable
  private final InputStats inputStats;
  private final RemoteHttpClient httpClient;
  @Nullable
  private RemoteHttpClient.Response response;
  private final ObjectReader rowReader;
  @Nullable
  private RemoteLineReader body;
  @Nullable
  private String sqlQueryId;
  private boolean sourceRunning;
  @Nullable
  private Map<String, Object> nextRow;
  private int attempts;
  private long rowsReturned;
  private boolean done;
  private boolean closed;

  RemoteResultIterator(
      final RemoteInputSource source,
      final ObjectMapper mapper,
      @Nullable final InputStats inputStats
  )
  {
    this.source = source;
    this.connection = source.getConnection();
    this.mapper = mapper;
    this.signature = Preconditions.checkNotNull(source.getSignature(), "signature");
    this.inputStats = inputStats;
    this.httpClient = connection.createClient(connection.getReadTimeout());
    this.rowReader = mapper.readerFor(ROW_TYPE);
    this.jsonFactory = mapper.getFactory();
    this.columnNames = signature.getColumnNames().toArray(new String[0]);
    this.stringColumns = new boolean[columnNames.length];
    this.timestampColumns = new boolean[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      stringColumns[i] = ColumnType.STRING.equals(signature.getColumnType(i).orElse(null));
      timestampColumns[i] = source.getTimestampColumns().contains(columnNames[i]);
    }
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
        if (!body.nextLine()) {
          throw new IOException("Remote Druid result ended without its completion marker");
        }
        if (body.lineLength() == 0) {
          if (body.hasMoreData()) {
            throw new RemoteNonRetryableException("Remote Druid result has data after its completion marker");
          }
          done = true;
          closeResponse(true);
          return false;
        }
        if (inputStats != null) {
          inputStats.incrementProcessedBytes(body.lineLength() + 1);
        }
        nextRow = decodeRow(body.buffer(), body.lineStart(), body.lineLength());
        return true;
      }
      catch (IOException e) {
        closeResponse(false);
        if (rowsReturned > 0 || !isRetryable(e) || attempts > connection.getMaxRetries()) {
          close();
          throw readFailure(e);
        }
        LOG.noStackTrace().warn(
            e,
            "Retrying remote Druid read of datasource[%s], range[%s]",
            source.getDataSource(),
            source.getRange()
        );
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
    final byte[] request = mapper.writeValueAsBytes(source.makeRequest(sqlQueryId));

    // The source may start the query even if its response never arrives, so it is canceled unless it was rejected.
    sourceRunning = true;
    try {
      response = httpClient.execute(endpoint, "POST", request);
    }
    catch (RemoteHttpClient.NotSentException e) {
      sourceRunning = false;
      throw e;
    }
    if (response.status() != 200) {
      sourceRunning = false;
      throw new RemoteStatusException(
          endpoint,
          response.status(),
          RemoteStatusException.readMessage(response.body(), mapper)
      );
    }
    body = new RemoteLineReader(response.body());
    if (!body.nextLine()) {
      throw new IOException("Remote Druid result ended before its header");
    }
    final List<Object> columns = rowReader.readValue(body.buffer(), body.lineStart(), body.lineLength());

    // Rows are mapped by position, so the source must return the planned columns in the planned order.
    if (!signature.getColumnNames().equals(columns)) {
      throw new RemoteNonRetryableException(
          "Remote Druid result has columns " + columns + ", but " + signature.getColumnNames() + " were planned"
      );
    }
  }

  /**
   * Decodes one result row, a JSON array with one value per planned column, with a streaming parser rather than
   * through a {@code List<Object>}. Values are the ones Jackson's untyped decoding produces, adjusted as the reader
   * requires:
   * <ul>
   * <li>timestamps that the source returns as text are parsed to epoch milliseconds;</li>
   * <li>booleans become 1 or 0, as Druid stores them;</li>
   * <li>string columns, which are requested as {@code MV_TO_ARRAY}, map an array to null when empty, to its element
   * when it has one, and to a multi-value list otherwise.</li>
   * </ul>
   */
  @VisibleForTesting
  Map<String, Object> decodeRow(final byte[] buffer, final int offset, final int length) throws IOException
  {
    try (final JsonParser parser = jsonFactory.createParser(buffer, offset, length)) {
      final JsonToken first = parser.nextToken();
      if (first != JsonToken.START_ARRAY) {
        throw new RemoteNonRetryableException(
            first == JsonToken.VALUE_NULL
            ? "Remote Druid result row has an unexpected number of columns"
            : "Remote Druid result row is not a JSON array"
        );
      }
      final Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 0; i < columnNames.length; i++) {
        final JsonToken token = parser.nextToken();
        if (token == JsonToken.END_ARRAY) {
          throw new RemoteNonRetryableException("Remote Druid result row has an unexpected number of columns");
        }
        row.put(columnNames[i], decodeValue(parser, token, i));
      }
      if (parser.nextToken() != JsonToken.END_ARRAY) {
        throw new RemoteNonRetryableException("Remote Druid result row has an unexpected number of columns");
      }
      if (parser.nextToken() != null) {
        throw new RemoteNonRetryableException("Remote Druid result row has data after its end");
      }
      return row;
    }
  }

  @Nullable
  private Object decodeValue(final JsonParser parser, final JsonToken token, final int column) throws IOException
  {
    switch (token) {
      case VALUE_NULL:
        return null;
      case VALUE_TRUE:
        return 1L;
      case VALUE_FALSE:
        return 0L;
      case VALUE_NUMBER_INT:
      case VALUE_NUMBER_FLOAT:
        return parser.getNumberValue();
      case VALUE_STRING:
        if (timestampColumns[column]) {
          try {
            return DateTimes.of(parser.getText()).getMillis();
          }
          catch (IllegalArgumentException e) {
            throw new RemoteNonRetryableException(
                "Remote Druid result has an invalid timestamp in column[" + columnNames[column] + "]"
            );
          }
        }
        return parser.getText();
      case START_ARRAY:
        final List<Object> elements = decodeArray(parser, column);
        if (stringColumns[column]) {
          // String columns are requested as MV_TO_ARRAY, so that multi-value rows arrive as arrays.
          return elements.isEmpty() ? null : elements.size() == 1 ? elements.getFirst() : elements;
        }
        return elements;
      default:
        throw new RemoteNonRetryableException(
            "Remote Druid result has an unsupported value in column[" + columnNames[column] + "]"
        );
    }
  }

  /**
   * Decodes the elements of an array value, after its opening token, as Jackson's untyped decoding would.
   */
  private List<Object> decodeArray(final JsonParser parser, final int column) throws IOException
  {
    final List<Object> elements = new ArrayList<>();
    for (JsonToken token = parser.nextToken(); token != JsonToken.END_ARRAY; token = parser.nextToken()) {
      switch (token) {
        case VALUE_NULL:
          elements.add(null);
          break;
        case VALUE_TRUE:
        case VALUE_FALSE:
          elements.add(token == JsonToken.VALUE_TRUE);
          break;
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
          elements.add(parser.getNumberValue());
          break;
        case VALUE_STRING:
          elements.add(parser.getText());
          break;
        case START_ARRAY:
          elements.add(decodeArray(parser, column));
          break;
        default:
          throw new RemoteNonRetryableException(
              "Remote Druid result has an unsupported value in column[" + columnNames[column] + "]"
          );
      }
    }
    return elements;
  }

  private void closeResponse(final boolean complete)
  {
    if (response != null) {
      response.close();
      response = null;
      body = null;
    }
    if (!complete && sourceRunning) {
      cancel(sqlQueryId);
    }
    sourceRunning = false;
  }

  private void cancel(final String queryId)
  {
    // Closing the connection also stops the source from streaming more results.
    connection.cancel(sqlEndpoint(), queryId);
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

  private URI sqlEndpoint()
  {
    return URI.create(connection.getEndpoint() + "/sql");
  }

  private static boolean isRetryable(final IOException e)
  {
    if (e instanceof RemoteNonRetryableException
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
   * Reports a read failure that the query must change to fix, such as rejected credentials or invalid source SQL, as
   * invalid input, and a corrupt result as a runtime failure, so that neither is retried by relaunching the reader.
   * Other failures, such as a broken connection, are I/O errors.
   */
  private static RuntimeException readFailure(final IOException e)
  {
    if (e instanceof RemoteSchemaException || (e instanceof RemoteStatusException status && status.isUserError())) {
      return InvalidInput.exception(e, "Remote Druid read failed: %s", e.getMessage());
    }
    if (e instanceof RemoteNonRetryableException || e instanceof JsonProcessingException) {
      return DruidException.forPersona(DruidException.Persona.USER)
                           .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                           .build(e, "Remote Druid read failed: %s", e.getMessage());
    }
    return new UncheckedIOException(e);
  }

  static void checkInterrupted() throws InterruptedIOException
  {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Remote Druid read interrupted");
    }
  }
}
