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

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

public class RemoteDruidTlsContextTest
{
  private static final String KEY_STORE_PASSWORD = "abc123";

  private final String keyStorePath = getClass().getClassLoader().getResource("keystore.jks").getFile();

  @Test
  public void testUsesConfiguredClientContextWhenBound() throws NoSuchAlgorithmException
  {
    final SSLContext configured = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, KEY_STORE_PASSWORD);
    final Injector withContext = Guice.createInjector(
        binder -> binder.bind(SSLContext.class).annotatedWith(Global.class).toInstance(configured)
    );
    Assertions.assertSame(configured, withContext.getInstance(RemoteDruidTlsContext.class).get());

    final Injector withoutContext = Guice.createInjector();
    Assertions.assertSame(SSLContext.getDefault(), withoutContext.getInstance(RemoteDruidTlsContext.class).get());
  }

  @Test
  public void testConnectsToSourceSignedByConfiguredTrustStore() throws Exception
  {
    final Server server = new Server();
    final HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());
    final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword(KEY_STORE_PASSWORD);
    sslContextFactory.setKeyManagerPassword(KEY_STORE_PASSWORD);
    final ServerConnector connector = new ServerConnector(
        server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)
    );
    connector.setPort(0);
    server.setConnectors(new Connector[]{connector});
    server.start();
    try {
      final URI endpoint = URI.create(StringUtils.format("https://localhost:%d/druid/v2", connector.getLocalPort()));

      // Trusted through the configured context: the TLS handshake succeeds and the server answers.
      final RemoteDruidTlsContext trusting = new RemoteDruidTlsContext(
          HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, KEY_STORE_PASSWORD)
      );
      try (final RemoteDruidHttpClient client = connection(endpoint, trusting).createClient(5000);
           final RemoteDruidHttpClient.Response response = client.execute(endpoint, "GET", null)) {
        Assertions.assertEquals(404, response.status());
      }

      // The JVM default does not trust the self-signed certificate.
      final RemoteDruidTlsContext jvmDefault = new RemoteDruidTlsContext(null);
      try (final RemoteDruidHttpClient client = connection(endpoint, jvmDefault).createClient(5000)) {
        final IOException e = Assertions.assertThrows(
            IOException.class,
            () -> client.execute(endpoint, "GET", null).close()
        );
        Assertions.assertTrue(hasCause(e, SSLException.class), String.valueOf(e));
      }
    }
    finally {
      server.stop();
    }
  }

  private static boolean hasCause(final Throwable throwable, final Class<? extends Throwable> type)
  {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  private static RemoteDruidConnection connection(final URI endpoint, final RemoteDruidTlsContext tlsContext)
  {
    return new RemoteDruidConnection(endpoint, null, 5000, 5000, 0, tlsContext);
  }
}
