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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Reads newline-terminated lines from a stream into a reusable buffer, with bulk reads. A line is valid, as
 * {@link #buffer()} from {@link #lineStart()} for {@link #lineLength()} bytes without the terminator, until the next
 * call to {@link #nextLine()} or {@link #hasMoreData()}.
 */
final class RemoteLineReader
{
  static final int MAX_LINE_BYTES = 16 * 1024 * 1024;
  private static final int INITIAL_BUFFER_BYTES = 64 * 1024;

  private final InputStream in;
  private final int maxLineBytes;
  private byte[] buffer;

  // Unconsumed bytes are buffer[start, end).
  private int start;
  private int end;
  private int lineStart;
  private int lineLength;

  RemoteLineReader(final InputStream in)
  {
    this(in, INITIAL_BUFFER_BYTES, MAX_LINE_BYTES);
  }

  RemoteLineReader(final InputStream in, final int initialBufferBytes, final int maxLineBytes)
  {
    this.in = in;
    this.maxLineBytes = maxLineBytes;
    this.buffer = new byte[Math.min(initialBufferBytes, maxLineBytes + 1)];
  }

  /**
   * Advances to the next line, or returns false if the stream ends before another line starts.
   *
   * @throws IOException if the stream ends in the middle of a line, or a line exceeds the maximum length
   */
  boolean nextLine() throws IOException
  {
    int scan = start;
    while (true) {
      for (; scan < end; scan++) {
        if (buffer[scan] == '\n') {
          lineStart = start;
          lineLength = scan - start;
          start = scan + 1;
          RemoteResultIterator.checkInterrupted();
          return true;
        }
      }
      if (end - start > maxLineBytes) {
        throw new RemoteNonRetryableException("Remote Druid result row exceeds " + maxLineBytes + " bytes");
      }
      if (start > 0) {
        // Move the partial line to the front, so that the rest of the buffer can be filled.
        System.arraycopy(buffer, start, buffer, 0, end - start);
        scan -= start;
        end -= start;
        start = 0;
      }
      if (end == buffer.length) {
        // One more byte than the limit is enough to tell that a line is too long.
        buffer = Arrays.copyOf(buffer, (int) Math.min(2L * buffer.length, maxLineBytes + 1L));
      }
      final int read = in.read(buffer, end, buffer.length - end);
      if (read < 0) {
        if (start == end) {
          return false;
        }
        throw new IOException("Remote Druid result ended in the middle of a row");
      }
      end += read;
    }
  }

  /**
   * Returns whether any data follows the current line.
   */
  boolean hasMoreData() throws IOException
  {
    if (start < end) {
      return true;
    }
    start = 0;
    end = 0;
    final int read = in.read(buffer, 0, buffer.length);
    if (read > 0) {
      end = read;
    }
    return read > 0;
  }

  byte[] buffer()
  {
    return buffer;
  }

  int lineStart()
  {
    return lineStart;
  }

  int lineLength()
  {
    return lineLength;
  }
}
