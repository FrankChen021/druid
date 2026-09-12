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
import com.fasterxml.jackson.core.JsonParser;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.io.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

final class JsonReaderUtils
{
  private JsonReaderUtils()
  {
  }

  static JsonParser createParser(final JsonFactory factory, final InputEntity source) throws IOException
  {
    // Inspect the opened stream, rather than the entity, to preserve decompression and custom open() behavior.
    final InputStream stream = source.open();
    try {
      if (stream instanceof ByteBufferInputStream) {
        final ByteBuffer buffer = ((ByteBufferInputStream) stream).getBuffer();
        if (buffer.hasArray()) {
          stream.close();
          return factory.createParser(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
      }
      return factory.createParser(stream);
    }
    catch (IOException | RuntimeException | Error e) {
      try {
        stream.close();
      }
      catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }
}
