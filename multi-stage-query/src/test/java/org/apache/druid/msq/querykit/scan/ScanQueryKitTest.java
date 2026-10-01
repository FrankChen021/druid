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

package org.apache.druid.msq.querykit.scan;

import org.apache.druid.msq.input.system.SystemTableInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.scan.ScanQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ScanQueryKitTest
{
  /** An unordered LIMIT/OFFSET may read only enough rows from each source to satisfy the global result. */
  @Test
  public void testSourceLimitForUnorderedScan()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .eternityInterval()
                                  .offset(3)
                                  .limit(7)
                                  .build();

    Assertions.assertEquals(10, ScanQueryKit.sourceLimit(query));
  }

  /** ORDER BY requires every source row to reach the global sort, so LIMIT/OFFSET is not pushed to sources. */
  @Test
  public void testSourceLimitForOrderedScan()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .eternityInterval()
                                  .orderBy(List.of(new OrderBy("value", Order.DESCENDING)))
                                  .offset(3)
                                  .limit(7)
                                  .build();

    Assertions.assertEquals(Long.MAX_VALUE, ScanQueryKit.sourceLimit(query));
  }

  /** An unordered system-table scan may use an unsorted mix shuffle instead of sorting by partition boost. */
  @Test
  public void testUnorderedSystemTableScanCanUseMixShuffle()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .eternityInterval()
                                  .build();

    Assertions.assertTrue(
        ScanQueryKit.canUseMixShuffleForUnorderedSystemTableScan(
            List.of(new SystemTableInputSpec("server_properties")),
            query
        )
    );
  }

  /** An ordered system-table scan must retain its global-sort shuffle. */
  @Test
  public void testOrderedSystemTableScanCannotUseMixShuffle()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .eternityInterval()
                                  .orderBy(List.of(new OrderBy("value", Order.ASCENDING)))
                                  .build();

    Assertions.assertFalse(
        ScanQueryKit.canUseMixShuffleForUnorderedSystemTableScan(
            List.of(new SystemTableInputSpec("server_properties")),
            query
        )
    );
  }

  /** Ordinary datasource scans retain the generic ScanQueryKit shuffle behavior. */
  @Test
  public void testOrdinaryScanCannotUseSystemTableMixShuffle()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("foo")
                                  .eternityInterval()
                                  .build();

    Assertions.assertFalse(
        ScanQueryKit.canUseMixShuffleForUnorderedSystemTableScan(
            List.of(new TableInputSpec("foo", null, null)),
            query
        )
    );
  }
}
