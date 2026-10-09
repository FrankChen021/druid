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

import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Bitmap32ExactCountMergeAggregatorFactoryTest
{
  private static final String NAME = "exactCountMergeTestName";
  private static final String FIELD_NAME = "exactCountMergeTestFieldName";

  private Bitmap32ExactCountMergeAggregatorFactory factory;

  @BeforeEach
  public void setUp()
  {
    factory = new Bitmap32ExactCountMergeAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void testConstructor()
  {
    Assertions.assertEquals(NAME, factory.getName());
    Assertions.assertEquals(FIELD_NAME, factory.getFieldName());
  }

  @Test
  public void testGetCacheTypeId()
  {
    Assertions.assertEquals(AggregatorUtil.BITMAP32_EXACT_COUNT_MERGE_CACHE_TYPE_ID, factory.getCacheTypeId());
  }

  @Test
  public void testFactorize()
  {
    ColumnSelectorFactory selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeColumnValueSelector(FIELD_NAME))
            .andReturn(new TestObjectColumnSelector<Bitmap32>(null)); // Return a dummy selector
    EasyMock.replay(selectorFactory);

    Assertions.assertInstanceOf(Bitmap32ExactCountMergeAggregator.class, factory.factorize(selectorFactory));
    EasyMock.verify(selectorFactory);
  }

  @Test
  public void testFactorizeBuffered()
  {
    ColumnSelectorFactory selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeColumnValueSelector(FIELD_NAME))
            .andReturn(new TestObjectColumnSelector<Bitmap32>(null)); // Return a dummy selector
    EasyMock.replay(selectorFactory);

    Assertions.assertInstanceOf(
        Bitmap32ExactCountMergeBufferAggregator.class,
        factory.factorizeBuffered(selectorFactory)
    );
    EasyMock.verify(selectorFactory);
  }

  @Test
  public void testGetIntermediateType()
  {
    Assertions.assertEquals(Bitmap32ExactCountMergeAggregatorFactory.TYPE, factory.getIntermediateType());
  }

  @Test
  public void testGetResultType()
  {
    Assertions.assertEquals(ColumnType.LONG, factory.getResultType());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    Bitmap32ExactCountMergeAggregatorFactory factory1 = new Bitmap32ExactCountMergeAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Bitmap32ExactCountMergeAggregatorFactory factory2 = new Bitmap32ExactCountMergeAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Bitmap32ExactCountMergeAggregatorFactory factoryDiffName = new Bitmap32ExactCountMergeAggregatorFactory(
        NAME + "_diff",
        FIELD_NAME
    );
    Bitmap32ExactCountMergeAggregatorFactory factoryDiffFieldName = new Bitmap32ExactCountMergeAggregatorFactory(
        NAME,
        FIELD_NAME + "_diff"
    );

    Assertions.assertEquals(factory1, factory2);
    Assertions.assertEquals(factory1.hashCode(), factory2.hashCode());

    Assertions.assertNotEquals(factory1, factoryDiffName);
    Assertions.assertNotEquals(factory1.hashCode(), factoryDiffName.hashCode());

    Assertions.assertNotEquals(factory1, factoryDiffFieldName);
    Assertions.assertNotEquals(factory1.hashCode(), factoryDiffFieldName.hashCode());
  }

  @Test
  public void testToString()
  {
    String expected = "Bitmap32ExactCountMergeAggregatorFactory { name="
                      + NAME
                      + ", fieldName="
                      + FIELD_NAME
                      + " }";
    Assertions.assertEquals(expected, factory.toString());
  }
}
