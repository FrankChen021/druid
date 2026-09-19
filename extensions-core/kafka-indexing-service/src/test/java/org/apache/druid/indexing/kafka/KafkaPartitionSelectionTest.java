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

package org.apache.druid.indexing.kafka;

import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class KafkaPartitionSelectionTest
{
  @Test
  public void testDiscoveryValidatesSelectionAndRecovers()
  {
    final KafkaConsumer<byte[], byte[]> consumer = createMock(KafkaConsumer.class);
    expect(consumer.partitionsFor("events")).andReturn(List.of(partition(0), partition(1)));
    expect(consumer.partitionsFor("events")).andReturn(List.of(partition(0), partition(1), partition(2)));
    expect(consumer.partitionsFor("events")).andReturn(List.of(partition(0), partition(1), partition(2), partition(3)));
    replay(consumer);
    final KafkaRecordSupplier supplier = new KafkaRecordSupplier(consumer, false, null, Set.of(0, 2));
    final StreamException error = Assertions.assertThrows(StreamException.class, () -> supplier.getPartitionIds("events"));
    Assertions.assertTrue(error.getMessage().contains("[2]"));
    final Set<KafkaTopicPartition> selected = Set.of(
        new KafkaTopicPartition(false, "events", 0), new KafkaTopicPartition(false, "events", 2)
    );
    Assertions.assertEquals(selected, supplier.getPartitionIds("events"));
    Assertions.assertEquals(selected, supplier.getPartitionIds("events"));
    verify(consumer);
  }

  private static PartitionInfo partition(int id)
  {
    return new PartitionInfo("events", id, null, new Node[0], new Node[0]);
  }
}
