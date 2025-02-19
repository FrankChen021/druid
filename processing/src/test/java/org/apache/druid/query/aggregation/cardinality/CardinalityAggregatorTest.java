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

package org.apache.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CardinalityAggregatorTest extends InitializedNullHandlingTest
{
  public static class TestDimensionSelector extends AbstractDimensionSelector
  {
    private final List<Integer[]> column;
    private final Map<String, Integer> ids;
    private final Map<Integer, String> lookup;
    private final ExtractionFn exFn;

    private int pos = 0;

    public TestDimensionSelector(Iterable<String[]> values, ExtractionFn exFn)
    {
      this.lookup = new HashMap<>();
      this.ids = new HashMap<>();
      this.exFn = exFn;

      int index = 0;
      for (String[] multiValue : values) {
        for (String value : multiValue) {
          if (!ids.containsKey(value)) {
            ids.put(value, index);
            lookup.put(index, value);
            index++;
          }
        }
      }

      this.column = Lists.newArrayList(
          Iterables.transform(
              values,
              new Function<>()
              {
                @Nullable
                @Override
                public Integer[] apply(@Nullable String[] input)
                {
                  return Iterators.toArray(Iterators.transform(Iterators.forArray(input), ids::get), Integer.class);
                }
              }
          )
      );
    }

    public void increment()
    {
      pos++;
    }

    public void reset()
    {
      pos = 0;
    }

    @Override
    public IndexedInts getRow()
    {
      final int p = this.pos;
      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return column.get(p).length;
        }

        @Override
        public int get(int i)
        {
          return column.get(p)[i];
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Don't care about runtime shape in tests
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(String value)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
    }

    @Override
    public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    public String lookupName(int i)
    {
      String val = lookup.get(i);
      return exFn == null ? val : exFn.apply(val);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return new IdLookup()
      {
        @Override
        public int lookupId(String s)
        {
          return ids.get(s);
        }
      };
    }

    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Don't care about runtime shape in tests
    }
  }

  /*
    values1: 4 distinct rows
    values1: 4 distinct values
    values2: 8 distinct rows
    values2: 7 distinct values
    groupBy(values1, values2): 9 distinct rows
    groupBy(values1, values2): 7 distinct values
    combine(values1, values2): 8 distinct rows
    combine(values1, values2): 7 distinct values
   */
  private static final List<String[]> VALUES1 = dimensionValues(
      "a", "b", "c", "a", "a", null, "b", "b", "b", "b", "a", "a"
  );
  private static final List<String[]> VALUES2 = dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{null, "x"},
      new String[]{"x", null},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );

  private static List<String[]> dimensionValues(Object... values)
  {
    return Lists.transform(
        Lists.newArrayList(values), new Function<>()
        {
          @Nullable
          @Override
          public String[] apply(@Nullable Object input)
          {
            if (input instanceof String[]) {
              return (String[]) input;
            } else {
              return new String[]{(String) input};
            }
          }
        }
    );
  }

  private static void aggregate(List<DimensionSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
  }

  private static void bufferAggregate(
      List<DimensionSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
  }

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoList;
  List<DimensionSelector> selectorList;
  CardinalityAggregatorFactory rowAggregatorFactory;
  CardinalityAggregatorFactory rowAggregatorFactoryRounded;
  CardinalityAggregatorFactory valueAggregatorFactory;
  final TestDimensionSelector dim1;
  final TestDimensionSelector dim2;

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoListWithExtraction;
  List<DimensionSelector> selectorListWithExtraction;
  final TestDimensionSelector dim1WithExtraction;
  final TestDimensionSelector dim2WithExtraction;

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoListConstantVal;
  List<DimensionSelector> selectorListConstantVal;
  final TestDimensionSelector dim1ConstantVal;
  final TestDimensionSelector dim2ConstantVal;

  final DimensionSpec dimSpec1 = new DefaultDimensionSpec("dim1", "dim1");
  final DimensionSpec dimSpec2 = new DefaultDimensionSpec("dim2", "dim2");

  public CardinalityAggregatorTest()
  {
    dim1 = new TestDimensionSelector(VALUES1, null);
    dim2 = new TestDimensionSelector(VALUES2, null);

    dimInfoList = Lists.newArrayList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        ),
        new ColumnSelectorPlus<>(
            dimSpec2.getDimension(),
            dimSpec2.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    rowAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList(
            dimSpec1,
            dimSpec2
        ),
        true
    );

    rowAggregatorFactoryRounded = new CardinalityAggregatorFactory(
        "billy",
        null,
        Lists.newArrayList(
            dimSpec1,
            dimSpec2
        ),
        true,
        true
    );

    valueAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList(
            dimSpec1,
            dimSpec2
        ),
        false
    );

    String superJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(superJsFn, false, JavaScriptConfig.getEnabledInstance());
    dim1WithExtraction = new TestDimensionSelector(VALUES1, superFn);
    dim2WithExtraction = new TestDimensionSelector(VALUES2, superFn);
    selectorListWithExtraction = Lists.newArrayList(dim1WithExtraction, dim2WithExtraction);
    dimInfoListWithExtraction = Lists.newArrayList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1WithExtraction
        ),
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2WithExtraction
        )
    );

    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());
    dim1ConstantVal = new TestDimensionSelector(VALUES1, helloFn);
    dim2ConstantVal = new TestDimensionSelector(VALUES2, helloFn);
    selectorListConstantVal = Lists.newArrayList(dim1ConstantVal, dim2ConstantVal);
    dimInfoListConstantVal = Lists.newArrayList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1ConstantVal
        ),
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2ConstantVal
        )
    );

  }

  @Test
  public void testAggregateRows()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoList,
        true
    );

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);
    Assert.assertEquals(9L, rowAggregatorFactoryRounded.finalizeComputation(agg.get()));
  }

  @Test
  public void testAggregateValues()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoList,
        false
    );

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(
        6.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg.get()),
        0.05
    );
    Assert.assertEquals(
        6L,
        rowAggregatorFactoryRounded.finalizeComputation(agg.get())
    );

  }

  @Test
  public void testBufferAggregateRows()
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        dimInfoList.toArray(new ColumnSelectorPlus[0]),
        true
    );

    int maxSize = rowAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < VALUES1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
    Assert.assertEquals(9L, rowAggregatorFactoryRounded.finalizeComputation(agg.get(buf, pos)));
  }

  @Test
  public void testBufferAggregateValues()
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        dimInfoList.toArray(new ColumnSelectorPlus[0]),
        false
    );

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < VALUES1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(
        6.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos)),
        0.05
    );
    Assert.assertEquals(
        6L,
        rowAggregatorFactoryRounded.finalizeComputation(agg.get(buf, pos))
    );
  }

  @Test
  public void testCombineRows()
  {
    List<DimensionSelector> selector1 = Collections.singletonList(dim1);
    List<DimensionSelector> selector2 = Collections.singletonList(dim2);
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo1 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        )
    );
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo2 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    CardinalityAggregator agg1 = new CardinalityAggregator(dimInfo1, true);
    CardinalityAggregator agg2 = new CardinalityAggregator(dimInfo2, true);

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < VALUES2.size(); ++i) {
      aggregate(selector2, agg2);
    }

    Assert.assertEquals(4.0, (Double) rowAggregatorFactory.finalizeComputation(agg1.get()), 0.05);
    Assert.assertEquals(8.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get()), 0.05);

    Assert.assertEquals(
        9.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testCombineValues()
  {
    List<DimensionSelector> selector1 = Collections.singletonList(dim1);
    List<DimensionSelector> selector2 = Collections.singletonList(dim2);

    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo1 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        )
    );
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo2 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    CardinalityAggregator agg1 = new CardinalityAggregator(dimInfo1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator(dimInfo2, false);

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < VALUES2.size(); ++i) {
      aggregate(selector2, agg2);
    }
    Assert.assertEquals(
        3.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg1.get()),
        0.05
    );
    Assert.assertEquals(
        6.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg2.get()),
        0.05
    );
    Assert.assertEquals(
        6.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testAggregateRowsWithExtraction()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoListWithExtraction,
        true
    );
    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorListWithExtraction, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        dimInfoListConstantVal,
        true
    );
    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorListConstantVal, agg2);
    }
    Assert.assertEquals(3.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get()), 0.05);
  }

  @Test
  public void testAggregateValuesWithExtraction()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoListWithExtraction,
        false
    );
    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorListWithExtraction, agg);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get()), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        dimInfoListConstantVal,
        false
    );
    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorListConstantVal, agg2);
    }
    Assert.assertEquals(1.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get()), 0.05);
  }

  @Test
  public void testSerde() throws Exception
  {
    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        null,
        ImmutableList.of(
            new DefaultDimensionSpec("b", "b"),
            new DefaultDimensionSpec("a", "a"),
            new DefaultDimensionSpec("c", "c")
        ),
        true,
        true
    );
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );

    String fieldNamesOnly = "{"
                            + "\"type\":\"cardinality\","
                            + "\"name\":\"billy\","
                            + "\"fields\":[\"b\",\"a\",\"c\"],"
                            + "\"byRow\":true,"
                            + "\"round\":true"
                            + "}";
    Assert.assertEquals(
        factory,
        objectMapper.readValue(fieldNamesOnly, AggregatorFactory.class)
    );

    CardinalityAggregatorFactory factory2 = new CardinalityAggregatorFactory(
        "billy",
        ImmutableList.of(
            new ExtractionDimensionSpec("b", "b", new RegexDimExtractionFn(".*", false, null)),
            new RegexFilteredDimensionSpec(new DefaultDimensionSpec("a", "a"), ".*"),
            new DefaultDimensionSpec("c", "c")
        ),
        true
    );

    Assert.assertEquals(
        factory2,
        objectMapper.readValue(objectMapper.writeValueAsString(factory2), AggregatorFactory.class)
    );
  }

  @Test
  public void testAggregateRowsIgnoreNulls()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoList,
        true
    );

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);
    Assert.assertEquals(9L, rowAggregatorFactoryRounded.finalizeComputation(agg.get()));
  }

  @Test
  public void testAggregateValuesIgnoreNulls()
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        dimInfoList,
        false
    );

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(
        6.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg.get()),
        0.05
    );
    Assert.assertEquals(
        6L,
        rowAggregatorFactoryRounded.finalizeComputation(agg.get())
    );
  }

  @Test
  public void testCombineValuesIgnoreNulls()
  {
    List<DimensionSelector> selector1 = Collections.singletonList(dim1);
    List<DimensionSelector> selector2 = Collections.singletonList(dim2);

    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo1 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        )
    );
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo2 = Collections.singletonList(
        new ColumnSelectorPlus<>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    CardinalityAggregator agg1 = new CardinalityAggregator(dimInfo1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator(dimInfo2, false);

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < VALUES2.size(); ++i) {
      aggregate(selector2, agg2);
    }
    Assert.assertEquals(
        3.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg1.get()),
        0.05
    );
    Assert.assertEquals(
        6.0,
        (Double) valueAggregatorFactory.finalizeComputation(agg2.get()),
        0.05
    );
    Assert.assertEquals(
        6.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }
}
