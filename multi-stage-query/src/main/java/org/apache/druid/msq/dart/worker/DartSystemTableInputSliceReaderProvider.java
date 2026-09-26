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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.InputSliceReaderProvider;
import org.apache.druid.msq.input.system.SystemTableInputSlice;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;

import java.util.Map;

/** Worker-side provider for Dart system-table inputs. */
public class DartSystemTableInputSliceReaderProvider implements InputSliceReaderProvider
{
  private final Injector injector;

  @Inject
  public DartSystemTableInputSliceReaderProvider(final Injector injector)
  {
    this.injector = injector;
  }

  @Override
  public Class<? extends InputSlice> sliceClass()
  {
    return SystemTableInputSlice.class;
  }

  @Override
  public InputSliceReader createReader(final FrameContext frameContext, final QueryContext queryContext)
  {
    // Resolve native system-table bindings only for this input type. Ordinary Dart worker tests do not need them.
    return new DartSystemTableInputSliceReader(
        injector.getInstance(Key.get(HttpClient.class, EscalatedGlobal.class)),
        injector.getInstance(ObjectMapper.class),
        injector.getInstance(Key.get(new TypeLiteral<Map<String, SystemTableDescriptor>>() {})),
        injector.getInstance(Key.get(new TypeLiteral<Map<String, SystemTableDataProvider>>() {})),
        injector.getInstance(Key.get(DruidNode.class, Self.class)),
        injector.getInstance(Escalator.class).createEscalatedAuthenticationResult(),
        injector.getInstance(AuthorizerMapper.class),
        queryContext
    );
  }
}
