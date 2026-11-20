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

import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RemoteLineReaderTest
{
  @Test
  public void testAcrossRefillsAndLongLines() throws IOException
  {
    final String text = "[1]\n" + "x".repeat(50) + "\n\n" + "[2]\n";

    // A stream that returns at most three bytes per read, and sometimes none, with a four-byte buffer, splits lines
    // across refills and makes the buffer grow for the long line.
    final InputStream trickle = new ByteArrayInputStream(StringUtils.toUtf8(text))
    {
      private boolean empty;

      @Override
      public synchronized int read(final byte[] bytes, final int offset, final int length)
      {
        // Every other read returns no bytes. InputStream does not do this for a nonzero length, but the reader should
        // still make progress if a stream does.
        empty = !empty;
        return empty ? 0 : super.read(bytes, offset, Math.min(length, 3));
      }
    };
    final RemoteLineReader reader = new RemoteLineReader(trickle, 4, 1000);
    final List<String> lines = new ArrayList<>();
    while (reader.nextLine()) {
      lines.add(StringUtils.fromUtf8(reader.buffer(), reader.lineStart(), reader.lineLength()));
    }
    Assertions.assertEquals(List.of("[1]", "x".repeat(50), "", "[2]"), lines);
  }

  @Test
  public void testLimitsAndTruncation() throws IOException
  {
    // A line may be exactly as long as the limit, but not longer.
    final RemoteLineReader exact = lineReader("12345\n", 5);
    Assertions.assertTrue(exact.nextLine());
    Assertions.assertEquals(5, exact.lineLength());
    final IOException tooLong = Assertions.assertThrows(IOException.class, () -> lineReader("123456\n", 5).nextLine());
    Assertions.assertTrue(tooLong.getMessage().contains("exceeds 5 bytes"), tooLong.getMessage());

    final IOException truncated = Assertions.assertThrows(IOException.class, () -> lineReader("[1]", 100).nextLine());
    Assertions.assertTrue(truncated.getMessage().contains("middle of a row"), truncated.getMessage());
  }

  private static RemoteLineReader lineReader(final String text, final int maxLineBytes)
  {
    return new RemoteLineReader(new ByteArrayInputStream(StringUtils.toUtf8(text)), 2, maxLineBytes);
  }
}
