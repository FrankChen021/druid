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

package org.apache.druid.metadata;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;

public class JUnit5TestDerbyConnector implements BeforeEachCallback, AfterEachCallback
{
  private TestDerbyConnector connector;
  private final MetadataStorageTablesConfig tablesConfig;
  private final MetadataStorageConnectorConfig connectorConfig;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public JUnit5TestDerbyConnector()
  {
    this(CentralizedDatasourceSchemaConfig.create());
  }

  public JUnit5TestDerbyConnector(final CentralizedDatasourceSchemaConfig config)
  {
    tablesConfig = MetadataStorageTablesConfig.fromBase("druidTest" + TestDerbyConnector.dbSafeUUID());
    connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return connector.getJdbcUri();
      }
    };
    centralizedDatasourceSchemaConfig = config;
  }

  @Override
  public void beforeEach(final ExtensionContext context)
  {
    before();
  }

  @Override
  public void afterEach(final ExtensionContext context)
  {
    after();
  }

  public void before()
  {
    if (connector == null) {
      connector = new TestDerbyConnector(connectorConfig, tablesConfig, centralizedDatasourceSchemaConfig);
      connector.createDatabase();
    }
  }

  public void after()
  {
    if (connector != null) {
      tearDown(connector);
      connector = null;
    }
  }

  public static void tearDown(final TestDerbyConnector connector)
  {
    try {
      new DBI(connector.getJdbcUri() + ";drop=true").open().close();
    }
    catch (UnableToObtainConnectionException e) {
      final SQLException cause = (SQLException) e.getCause();
      JUnit5Assertions.assertEquals("08006", cause.getSQLState(), "Derby not shutdown: " + cause);
    }
  }

  public TestDerbyConnector getConnector()
  {
    return connector;
  }

  public MetadataStorageConnectorConfig getMetadataConnectorConfig()
  {
    return connectorConfig;
  }

  public Supplier<MetadataStorageTablesConfig> metadataTablesConfigSupplier()
  {
    return Suppliers.ofInstance(tablesConfig);
  }

  public SegmentsTable segments()
  {
    return new SegmentsTable();
  }

  public class SegmentsTable
  {
    public int updateUsedStatusLastUpdated(final String segmentId, final DateTime lastUpdatedTime)
    {
      return connector.retryWithHandle(
          handle -> handle.update(
              "UPDATE " + tablesConfig.getSegmentsTable() + " SET USED_STATUS_LAST_UPDATED = ? WHERE ID = ?",
              lastUpdatedTime.toString(),
              segmentId
          )
      );
    }
  }
}
