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

package org.apache.druid.query.aggregation.exact.count.bitmap32;

import org.apache.druid.segment.ColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Bitmap32ExactCountMergeAggregatorTest
{
  private ColumnValueSelector<Bitmap32> mockSelector;
  private Bitmap32ExactCountMergeAggregator aggregator;

  @BeforeEach
  public void setUp()
  {
    mockSelector = EasyMock.createMock(ColumnValueSelector.class);
    aggregator = new Bitmap32ExactCountMergeAggregator(mockSelector);
  }

  @Test
  public void testAggregateWithNonNullCounter()
  {
    RoaringBitmap32Counter inputCounter = new RoaringBitmap32Counter();
    inputCounter.add(123);
    inputCounter.add(456);

    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate();

    Bitmap32 resultCounter = (Bitmap32) aggregator.get();
    Assertions.assertNotNull(resultCounter);
    Assertions.assertEquals(2, resultCounter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateWithNullCounter()
  {
    EasyMock.expect(mockSelector.getObject()).andReturn(null).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(); // Should not throw NPE, RoaringBitmap32Counter.fold handles null

    Bitmap32 resultCounter = (Bitmap32) aggregator.get();
    Assertions.assertNotNull(resultCounter);
    Assertions.assertEquals(0, resultCounter.getCardinality(), "Aggregating null should not change cardinality count");

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateMultipleCounters()
  {
    RoaringBitmap32Counter inputCounter1 = new RoaringBitmap32Counter();
    inputCounter1.add(10);
    inputCounter1.add(20);

    RoaringBitmap32Counter inputCounter2 = new RoaringBitmap32Counter();
    inputCounter2.add(20); // Duplicate with counter1
    inputCounter2.add(30);

    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter1).once();
    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter2).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate();
    aggregator.aggregate();

    Bitmap32 resultCounter = (Bitmap32) aggregator.get();
    Assertions.assertNotNull(resultCounter);
    Assertions.assertEquals(3, resultCounter.getCardinality()); // 10, 20, 30

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateCountersWithNegativeValues()
  {
    RoaringBitmap32Counter negativeHighCounter = new RoaringBitmap32Counter();
    negativeHighCounter.add(-1);

    RoaringBitmap32Counter nonNegativeHighCounter = new RoaringBitmap32Counter();
    nonNegativeHighCounter.add(0);
    nonNegativeHighCounter.add(1);

    EasyMock.expect(mockSelector.getObject()).andReturn(negativeHighCounter).once();
    EasyMock.expect(mockSelector.getObject()).andReturn(nonNegativeHighCounter).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate();
    aggregator.aggregate();

    Bitmap32 resultCounter = (Bitmap32) aggregator.get();
    Assertions.assertEquals(3, resultCounter.getCardinality());

    EasyMock.verify(mockSelector);
  }
  
  @Test
  public void testAggregateMultipleCountersIncludingNull()
  {
    RoaringBitmap32Counter inputCounter1 = new RoaringBitmap32Counter();
    inputCounter1.add(10);
    inputCounter1.add(20);

    RoaringBitmap32Counter inputCounter3 = new RoaringBitmap32Counter();
    inputCounter3.add(20); 
    inputCounter3.add(30);

    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter1).once();
    EasyMock.expect(mockSelector.getObject()).andReturn(null).once(); // Null counter
    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter3).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(); // counter1
    aggregator.aggregate(); // null
    aggregator.aggregate(); // counter3

    Bitmap32 resultCounter = (Bitmap32) aggregator.get();
    Assertions.assertNotNull(resultCounter);
    Assertions.assertEquals(3, resultCounter.getCardinality()); // 10, 20, 30

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testGetInitialState()
  {
    Bitmap32 counter = (Bitmap32) aggregator.get();
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(0, counter.getCardinality());
  }

  @Test
  public void testClose()
  {
    aggregator.close();
    Assertions.assertNull(aggregator.get(), "Bitmap should be null after close");
  }

  @Test
  public void testUnsupportedGetOperations()
  {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getFloat());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getLong());
  }
}
