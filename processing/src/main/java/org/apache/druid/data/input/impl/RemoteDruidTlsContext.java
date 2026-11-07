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

import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Global;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;

/// The TLS context for requests to remote Druid clusters: the server's configured client context when an extension
/// such as `simple-client-sslcontext` binds one, otherwise the JVM default. Guice creates it on demand, so it is
/// available wherever a remote Druid input source is deserialized.
public class RemoteDruidTlsContext
{
  @Inject(optional = true)
  @Global
  @Nullable
  private SSLContext configured;

  public RemoteDruidTlsContext()
  {
  }

  /// Uses the given context instead of an injected one.
  public RemoteDruidTlsContext(@Nullable final SSLContext configured)
  {
    this.configured = configured;
  }

  public SSLContext get()
  {
    if (configured != null) {
      return configured;
    }
    try {
      return SSLContext.getDefault();
    }
    catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("No default TLS context is available", e);
    }
  }
}
