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

import com.google.common.util.concurrent.ListenableFuture;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.metadata.PasswordProvider;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// Transport boundary allows authentication providers to perform more than header decoration.
public interface RemoteDruidHttpClient extends AutoCloseable
{
  /// Executes a request without following redirects. The caller must close the returned response.
  Response execute(URI endpoint, String method, @Nullable byte[] body) throws IOException;

  @Override
  default void close()
  {
  }

  /// A response whose body and transport resources remain valid until it is closed.
  interface Response extends AutoCloseable
  {
    int status();

    /// Returns a response header value when the transport exposes response metadata.
    @Nullable
    default String header(final String name)
    {
      return null;
    }

    InputStream body() throws IOException;

    @Override
    void close();
  }

  /// Uses Druid's pooled HTTP transport, with a lifecycle owned by this ingestion client.
  class DruidHttpClient implements RemoteDruidHttpClient
  {
    private final Lifecycle lifecycle = new Lifecycle();
    private final HttpClient client;
    @Nullable
    private final String username;
    @Nullable
    private final PasswordProvider password;

    public DruidHttpClient(
        @Nullable final String username,
        @Nullable final PasswordProvider password,
        final int connectTimeout,
        final int readTimeout
    )
    {
      this.username = username;
      this.password = password;
      try {
        client = HttpClientInit.createClient(
            HttpClientConfig.builder()
                            .withWorkerCount(1)
                            .withNumConnections(1)
                            .withEagerInitialization(false)
                            .withConnectTimeout(Duration.millis(connectTimeout))
                            .withReadTimeout(Duration.millis(readTimeout))
                            .withSslHandshakeTimeout(Duration.millis(connectTimeout))
                            .withSslContext(SSLContext.getDefault())
                            .withCompressionCodec(HttpClientConfig.CompressionCodec.IDENTITY)
                            .build(),
            lifecycle
        );
        lifecycle.start();
      }
      catch (Exception e) {
        lifecycle.stop();
        throw new IllegalStateException("Unable to initialize remote Druid HTTP transport", e);
      }
    }

    @Override
    public Response execute(final URI endpoint, final String method, @Nullable final byte[] body) throws IOException
    {
      final Request request = new Request(HttpMethod.valueOf(method), endpoint.toURL());
      if (username != null && password != null) {
        // Resolved per request, so that providers such as environment variables are read where the request runs.
        final String resolvedPassword = password.getPassword();
        if (resolvedPassword == null) {
          throw new IOException("Remote Druid password provider did not return a password on this server");
        }
        request.setBasicAuthentication(username, resolvedPassword);
      }
      if (body != null) {
        request.setContent("application/json", body);
      }
      final StreamingResponseHandler handler = new StreamingResponseHandler();
      try {
        final ListenableFuture<InputStreamFullResponseHolder> future = client.go(request, handler);
        final InputStreamFullResponseHolder holder;
        try {
          holder = future.get();
        }
        catch (InterruptedException e) {
          future.cancel(true);
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Remote Druid HTTP request interrupted");
        }
        catch (ExecutionException e) {
          throw new IOException("Remote Druid HTTP request failed", e.getCause());
        }
        return new Response()
        {
          private final InputStream stream = new FilterInputStream(holder.getContent())
          {
            @Override
            public int read() throws IOException
            {
              final int value = in.read();
              handler.resume(value < 0 ? 0 : 1);
              return value;
            }

            @Override
            public int read(final byte[] bytes, final int offset, final int length) throws IOException
            {
              Objects.checkFromIndexSize(offset, length, bytes.length);
              // The underlying stream fills the requested length. Keep it below the suspension threshold.
              final int count = in.read(bytes, offset, Math.min(length, StreamingResponseHandler.READ_SIZE));
              handler.resume(Math.max(0, count));
              return count;
            }

            @Override
            public long skip(final long count) throws IOException
            {
              final long skipped = in.skip(Math.min(count, StreamingResponseHandler.READ_SIZE));
              handler.resume(skipped);
              return skipped;
            }

            @Override
            public void close()
            {
              handler.close();
            }
          };

          @Override
          public int status()
          {
            return holder.getStatus().code();
          }

          @Override
          public String header(final String name)
          {
            return holder.getResponse().headers().get(name);
          }

          @Override
          public InputStream body()
          {
            return stream;
          }

          @Override
          public void close()
          {
            handler.close();
          }
        };
      }
      catch (IOException | RuntimeException e) {
        handler.close();
        if (e instanceof IOException io) {
          throw io;
        }
        throw new IOException("Remote Druid HTTP request failed", e);
      }
      finally {
        // Netty retains its own reference when submitting a request; release the caller's reference.
        if (request.hasContent()) {
          request.getContent().release();
        }
      }
    }

    @Override
    public void close()
    {
      lifecycle.stop();
    }
  }

  /// Adds backpressure and early-close cancellation to Druid's streaming response handler.
  /// Applies a fixed queue bound and aborts the HTTP request when its consumer closes early.
  static final class StreamingResponseHandler extends InputStreamFullResponseHandler
  {
    static final int READ_SIZE = 8192;
    static final int MAX_QUEUED_BYTES = 64 * 1024;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong queuedBytes = new AtomicLong();
    @Nullable
    private volatile TrafficCop trafficCop;
    private volatile long lastChunk;
    private volatile boolean complete;

    @Override
    public ClientResponse<InputStreamFullResponseHolder> handleResponse(
        final HttpResponse response,
        final TrafficCop cop
    )
    {
      trafficCop = cop;
      final ClientResponse<InputStreamFullResponseHolder> result = super.handleResponse(response, cop);
      if (closed.get()) {
        cop.abort();
      }
      return result;
    }

    @Override
    public ClientResponse<InputStreamFullResponseHolder> handleChunk(
        final ClientResponse<InputStreamFullResponseHolder> response,
        final HttpContent chunk,
        final long chunkNum
    )
    {
      lastChunk = chunkNum;
      if (closed.get()) {
        return ClientResponse.finished(response.getObj(), false);
      }
      queuedBytes.addAndGet(chunk.content().readableBytes());
      super.handleChunk(response, chunk, chunkNum);
      return ClientResponse.finished(response.getObj(), queuedBytes.get() < MAX_QUEUED_BYTES);
    }

    @Override
    public synchronized ClientResponse<InputStreamFullResponseHolder> done(
        final ClientResponse<InputStreamFullResponseHolder> response
    )
    {
      complete = true;
      return super.done(response);
    }

    synchronized void resume(final long consumed)
    {
      final long remaining = queuedBytes.addAndGet(-consumed);
      final TrafficCop cop = trafficCop;
      if (cop != null && !closed.get() && !complete && remaining < MAX_QUEUED_BYTES) {
        cop.resume(lastChunk);
      }
    }

    synchronized void close()
    {
      closed.set(true);
      final TrafficCop cop = trafficCop;
      if (cop != null && !complete) {
        cop.abort();
      }
    }

    long getQueuedBytes()
    {
      return queuedBytes.get();
    }
  }
}
