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

package org.apache.druid.msq.exec;

import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LiveFramesSessionTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder().add("value", ColumnType.LONG).build();

  @Test
  public void testPartitionsStayPinnedUntilExplicitRelease()
  {
    final LiveFramesSession session = readySession(new AtomicLong(1_000L));
    final LiveFramesSession.Manifest manifest = session.getInfo().getManifest();
    final LiveFramesSession.Partition first = manifest.getPartitions().get(0);

    Assertions.assertFalse(session.isOwnedBy("another-user"));
    Assertions.assertTrue(session.isOwnedBy("owner"));
    Assertions.assertEquals("worker-1", session.getPartitionLocation(first.getId()).getWorkerId());
    Assertions.assertEquals("ready", session.getInfo().getState());
    Assertions.assertEquals("worker-1", session.getPartitionLocation(first.getId()).getWorkerId());

    session.release();
    Assertions.assertEquals("released", session.getInfo().getState());
    Assertions.assertNull(session.getPartitionLocation(first.getId()));
  }

  @Test
  public void testLeaseRenewalIsBoundedAndExpiryMakesPartitionsUnavailable()
  {
    final AtomicLong now = new AtomicLong(1_000L);
    final LiveFramesSession session = readySession(now);
    final LiveFramesSession.Partition partition = session.getInfo().getManifest().getPartitions().get(0);
    final long maximumExpiry = now.get() + LiveFramesSession.MAX_LEASE_MILLIS;

    Assertions.assertTrue(session.renewLease(LiveFramesSession.MAX_LEASE_MILLIS));
    Assertions.assertEquals(maximumExpiry, session.getInfo().getLeaseExpiresAtMillis());
    Assertions.assertThrows(IllegalArgumentException.class, () -> session.renewLease(0));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> session.renewLease(LiveFramesSession.MAX_LEASE_MILLIS + 1)
    );

    now.set(maximumExpiry - 1);
    Assertions.assertTrue(session.renewLease(LiveFramesSession.MAX_LEASE_MILLIS));
    final long renewedExpiry = now.get() + LiveFramesSession.MAX_LEASE_MILLIS;
    Assertions.assertEquals(renewedExpiry, session.getInfo().getLeaseExpiresAtMillis());

    now.set(renewedExpiry);
    Assertions.assertEquals("expired", session.getInfo().getState());
    Assertions.assertNull(session.getPartitionLocation(partition.getId()));
    session.release();
    Assertions.assertEquals("released", session.getInfo().getState());
  }

  @Test
  public void testReleaseAndCancellationAreIdempotent()
  {
    final LiveFramesSession ready = readySession(new AtomicLong(1_000L));
    ready.release();
    ready.release();
    Assertions.assertEquals("released", ready.getInfo().getState());

    final LiveFramesSession submitted = new LiveFramesSession("query-2", "owner");
    submitted.cancel();
    submitted.cancel();
    Assertions.assertEquals("cancelled", submitted.getInfo().getState());
  }

  @Test
  public void testReleaseWaitsForActivePartitionResponseToClose() throws Exception
  {
    final LiveFramesSession session = readySession(new AtomicLong(1_000L));
    final String partitionId = session.getInfo().getManifest().getPartitions().get(0).getId();
    final LiveFramesSession.PartitionLocation location = session.beginPartitionRead(partitionId);
    Assertions.assertNotNull(location);
    Assertions.assertNotNull(location.getReadId());

    final FutureTask<Void> cleanupWait = new FutureTask<>(() -> {
      session.awaitRelease();
      return null;
    });
    final Thread cleanupThread = new Thread(cleanupWait);
    cleanupThread.start();
    session.release();

    Assertions.assertFalse(cleanupWait.isDone(), "worker output must stay alive while the response owns a read permit");
    Assertions.assertNull(session.beginPartitionRead(partitionId), "release must prevent new partition responses");
    session.endPartitionRead(location.getReadId());
    cleanupWait.get(1, TimeUnit.SECONDS);
    Assertions.assertTrue(cleanupWait.isDone());
  }

  @Test
  public void testExpiredPartitionReadIsBoundedByTheSessionLease() throws Exception
  {
    final AtomicLong now = new AtomicLong(1_000L);
    final LiveFramesSession session = readySession(now);
    final long sessionExpiry = session.getInfo().getLeaseExpiresAtMillis();
    now.set(sessionExpiry - TimeUnit.SECONDS.toMillis(5));
    final String partitionId = session.getInfo().getManifest().getPartitions().get(0).getId();
    final LiveFramesSession.PartitionLocation location = session.beginPartitionRead(partitionId);
    Assertions.assertNotNull(location);
    Assertions.assertEquals(sessionExpiry, location.getReadExpiresAtMillis());

    now.set(sessionExpiry);
    Assertions.assertEquals("expired", session.getInfo().getState());
    session.awaitRelease();
    Assertions.assertNull(session.beginPartitionRead(partitionId));
  }

  private static LiveFramesSession readySession(final AtomicLong now)
  {
    final LiveFramesSession session = new LiveFramesSession("query-1", "owner", now::get);
    session.markRunning();
    session.markReady(
        new StageId("query-1", 2),
        SIGNATURE,
        List.of(
            new LiveFramesSession.PartitionLocation("worker-1", 0),
            new LiveFramesSession.PartitionLocation("worker-2", 1)
        )
    );
    return session;
  }
}
