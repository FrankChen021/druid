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

package org.apache.druid.server.system.table;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Reads the rows of a system table in batches of the query's vector size, as columns, so that vectorized query engines
 * can process them.
 *
 * <p>Only the columns the query reads are copied into the batches. Rows are read from the authorized rows of
 * {@link SystemTableCursorFactory#makeAuthorizedRowSequence}, so the caller's access applies before rows reach the
 * query. Queries with a filter aren't vectorized: their filter is applied row by row.</p>
 */
class SystemTableVectorCursor implements VectorCursor, Closeable
{
  private final BatchOffset offset;
  private final VectorColumnSelectorFactory selectorFactory;

  SystemTableVectorCursor(
      final Supplier<Sequence<Object[]>> rows,
      final RowSignature signature,
      final CursorBuildSpec spec
  )
  {
    this.offset = new BatchOffset(rows, signature, spec.getPhysicalColumns(), spec.getQueryContext().getVectorSize());
    this.selectorFactory = new BatchVectorColumnSelectorFactory(offset, signature, spec.getVirtualColumns());
  }

  /**
   * Whether the query can read the table through this cursor: it has no filter, reads the whole table's interval, and
   * every column of the table, virtual column and aggregator it reads can be vectorized.
   */
  static boolean canVectorize(final CursorBuildSpec spec, final RowSignature signature)
  {
    if (spec.getFilter() != null || !Intervals.ETERNITY.equals(spec.getInterval())) {
      return false;
    }
    for (int column = 0; column < signature.size(); column++) {
      final ColumnType type = signature.getColumnType(column).orElse(null);
      if (!ColumnType.LONG.equals(type) && !ColumnType.STRING.equals(type)) {
        return false;
      }
    }
    final ColumnInspector inspector = new CapabilitiesInspector(signature);
    final VirtualColumns virtualColumns = spec.getVirtualColumns();
    if (!virtualColumns.isEmpty() && !virtualColumns.canVectorize(inspector)) {
      return false;
    }
    final List<AggregatorFactory> aggregators = spec.getAggregators();
    if (aggregators != null) {
      for (final AggregatorFactory aggregator : aggregators) {
        if (!aggregator.canVectorize(virtualColumns.wrapInspector(inspector))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public VectorColumnSelectorFactory getColumnSelectorFactory()
  {
    return selectorFactory;
  }

  @Override
  public void advance()
  {
    offset.advance();
    BaseQuery.checkInterrupted();
  }

  @Override
  public boolean isDone()
  {
    return offset.isDone();
  }

  @Override
  public void reset()
  {
    offset.reset();
  }

  @Override
  public int getMaxVectorSize()
  {
    return offset.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return offset.getCurrentVectorSize();
  }

  @Override
  public void close()
  {
    offset.close();
  }

  /**
   * Returns the capabilities that the cursor's selectors have for the given column of the table.
   */
  @Nullable
  static ColumnCapabilities getCapabilities(final RowSignature signature, final String column)
  {
    final ColumnType type = signature.getColumnType(column).orElse(null);
    if (ColumnType.STRING.equals(type)) {
      // The cursor's dimension selectors assign ids to values as they read them.
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(ColumnType.STRING)
                                   .setDictionaryEncoded(true)
                                   .setDictionaryValuesSorted(false)
                                   .setDictionaryValuesUnique(true)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    } else if (ColumnType.LONG.equals(type)) {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type).setHasNulls(true);
    }
    return null;
  }

  /**
   * Holds the current batch of rows, as one array per column the query reads.
   */
  private static class BatchOffset implements ReadableVectorOffset, Closeable
  {
    private final Supplier<Sequence<Object[]>> rows;
    private final RowSignature signature;
    private final int batchSize;
    private final int[] columns;
    private final long[][] longColumns;
    private final Object[][] objectColumns;
    private final boolean[][] nullColumns;

    @Nullable
    private Yielder<Object[]> yielder;
    private int currentSize;
    private int id;
    private boolean advanced;

    BatchOffset(
        final Supplier<Sequence<Object[]>> rows,
        final RowSignature signature,
        @Nullable final Set<String> queriedColumns,
        final int batchSize
    )
    {
      this.rows = rows;
      this.signature = signature;
      this.batchSize = batchSize;
      this.longColumns = new long[signature.size()][];
      this.objectColumns = new Object[signature.size()][];
      this.nullColumns = new boolean[signature.size()][];
      final List<Integer> readColumns = new ArrayList<>();
      for (int column = 0; column < signature.size(); column++) {
        if (queriedColumns == null || queriedColumns.contains(signature.getColumnName(column))) {
          readColumns.add(column);
        }
        // Columns the query doesn't read stay empty, which their selectors return as nulls.
        if (ColumnType.LONG.equals(signature.getColumnType(column).orElse(null))) {
          longColumns[column] = new long[batchSize];
          nullColumns[column] = new boolean[batchSize];
          Arrays.fill(nullColumns[column], true);
        } else {
          objectColumns[column] = new Object[batchSize];
        }
      }
      this.columns = readColumns.stream().mapToInt(Integer::intValue).toArray();
      this.yielder = Yielders.each(rows.get());
      try {
        loadBatch();
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, this);
      }
    }

    long[] getLongColumn(final int column)
    {
      return longColumns[column];
    }

    Object[] getObjectColumn(final int column)
    {
      return objectColumns[column];
    }

    boolean[] getNullColumn(final int column)
    {
      return nullColumns[column];
    }

    void advance()
    {
      advanced = true;
      loadBatch();
    }

    boolean isDone()
    {
      return currentSize == 0;
    }

    /**
     * Rewinds to the first batch. Reading the rows again can be expensive, for example when they come from a remote
     * response, so they are read again only if the offset has moved past the first batch, which it still holds
     * otherwise.
     */
    void reset()
    {
      if (advanced) {
        advanced = false;
        close();
        yielder = Yielders.each(rows.get());
        loadBatch();
      }
    }

    @Override
    public void close()
    {
      final Yielder<Object[]> currentYielder = yielder;
      yielder = null;
      CloseableUtils.closeAndWrapExceptions(currentYielder);
    }

    private void loadBatch()
    {
      int rowNumber = 0;
      while (rowNumber < batchSize && yielder != null && !yielder.isDone()) {
        final Object[] row = yielder.get();
        for (final int column : columns) {
          final Object value = row[column];
          if (longColumns[column] != null) {
            if (value != null && !(value instanceof Number)) {
              throw invalidValue(column, value);
            }
            nullColumns[column][rowNumber] = value == null;
            longColumns[column][rowNumber] = value == null ? 0L : ((Number) value).longValue();
          } else {
            if (value != null && !(value instanceof String)) {
              throw invalidValue(column, value);
            }
            objectColumns[column][rowNumber] = value;
          }
        }
        rowNumber++;
        yielder = yielder.next(null);
      }
      currentSize = rowNumber;
      id++;
    }

    private DruidException invalidValue(final int column, final Object value)
    {
      return DruidException.defensive(
          "Column[%s] of type[%s] has a value of class[%s]",
          signature.getColumnName(column),
          signature.getColumnType(column).orElse(null),
          value.getClass().getName()
      );
    }

    @Override
    public int getId()
    {
      return id;
    }

    @Override
    public boolean isContiguous()
    {
      return true;
    }

    @Override
    public int getMaxVectorSize()
    {
      return batchSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentSize;
    }

    @Override
    public int getStartOffset()
    {
      return 0;
    }

    @Override
    public int[] getOffsets()
    {
      throw DruidException.defensive("A contiguous batch has no offsets");
    }
  }

  private static class BatchVectorColumnSelectorFactory implements VectorColumnSelectorFactory
  {
    private final BatchOffset offset;
    private final RowSignature signature;
    private final VirtualColumns virtualColumns;
    private final Map<DimensionSpec, SingleValueDimensionVectorSelector> dimensionSelectors = new HashMap<>();
    private final Map<String, VectorValueSelector> valueSelectors = new HashMap<>();
    private final Map<String, VectorObjectSelector> objectSelectors = new HashMap<>();

    BatchVectorColumnSelectorFactory(
        final BatchOffset offset,
        final RowSignature signature,
        final VirtualColumns virtualColumns
    )
    {
      this.offset = offset;
      this.signature = signature;
      this.virtualColumns = virtualColumns;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return offset;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      SingleValueDimensionVectorSelector selector = dimensionSelectors.get(dimensionSpec);
      if (selector == null) {
        if (virtualColumns.exists(dimensionSpec.getDimension())) {
          selector = virtualColumns.makeSingleValueDimensionVectorSelector(dimensionSpec, this, null, offset);
        } else {
          final int column = signature.indexOf(dimensionSpec.getDimension());
          selector = column < 0
                     ? NilVectorSelector.create(offset)
                     : dimensionSpec.decorate(new BatchStringDimensionSelector(offset, offset.getObjectColumn(column)));
        }
        dimensionSelectors.put(dimensionSpec, selector);
      }
      return selector;
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw DruidException.defensive("System table columns don't have multiple values");
    }

    @Override
    public VectorValueSelector makeValueSelector(final String column)
    {
      VectorValueSelector selector = valueSelectors.get(column);
      if (selector == null) {
        if (virtualColumns.exists(column)) {
          selector = virtualColumns.makeVectorValueSelector(column, this, null, offset);
        } else {
          final int columnNumber = signature.indexOf(column);
          selector = columnNumber < 0
                     ? NilVectorSelector.create(offset)
                     : new BatchLongVectorValueSelector(
                         offset,
                         offset.getLongColumn(columnNumber),
                         offset.getNullColumn(columnNumber)
                     );
        }
        valueSelectors.put(column, selector);
      }
      return selector;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(final String column)
    {
      VectorObjectSelector selector = objectSelectors.get(column);
      if (selector == null) {
        if (virtualColumns.exists(column)) {
          selector = virtualColumns.makeVectorObjectSelector(column, this, null, offset);
        } else {
          final int columnNumber = signature.indexOf(column);
          selector = columnNumber < 0
                     ? NilVectorSelector.create(offset)
                     : new BatchObjectVectorSelector(offset, offset.getObjectColumn(columnNumber));
        }
        objectSelectors.put(column, selector);
      }
      return selector;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return virtualColumns.getColumnCapabilitiesWithFallback(new CapabilitiesInspector(signature), column);
    }
  }

  private static class BatchLongVectorValueSelector implements VectorValueSelector
  {
    private final ReadableVectorInspector inspector;
    private final long[] longs;
    private final boolean[] nulls;
    private final float[] floats;
    private final double[] doubles;
    private int floatId = ReadableVectorInspector.NULL_ID;
    private int doubleId = ReadableVectorInspector.NULL_ID;

    BatchLongVectorValueSelector(
        final ReadableVectorInspector inspector,
        final long[] longs,
        final boolean[] nulls
    )
    {
      this.inspector = inspector;
      this.longs = longs;
      this.nulls = nulls;
      this.floats = new float[inspector.getMaxVectorSize()];
      this.doubles = new double[inspector.getMaxVectorSize()];
    }

    @Override
    public long[] getLongVector()
    {
      return longs;
    }

    @Override
    public float[] getFloatVector()
    {
      if (floatId != inspector.getId()) {
        for (int i = 0; i < inspector.getCurrentVectorSize(); i++) {
          floats[i] = longs[i];
        }
        floatId = inspector.getId();
      }
      return floats;
    }

    @Override
    public double[] getDoubleVector()
    {
      if (doubleId != inspector.getId()) {
        for (int i = 0; i < inspector.getCurrentVectorSize(); i++) {
          doubles[i] = longs[i];
        }
        doubleId = inspector.getId();
      }
      return doubles;
    }

    @Override
    public boolean[] getNullVector()
    {
      return nulls;
    }

    @Override
    public int getMaxVectorSize()
    {
      return inspector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return inspector.getCurrentVectorSize();
    }
  }

  private static class BatchObjectVectorSelector implements VectorObjectSelector
  {
    private final ReadableVectorInspector inspector;
    private final Object[] objects;

    BatchObjectVectorSelector(final ReadableVectorInspector inspector, final Object[] objects)
    {
      this.inspector = inspector;
      this.objects = objects;
    }

    @Override
    public Object[] getObjectVector()
    {
      return objects;
    }

    @Override
    public int getMaxVectorSize()
    {
      return inspector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return inspector.getCurrentVectorSize();
    }
  }

  /**
   * A string selector that assigns ids to values in the order it first reads them.
   */
  private static class BatchStringDimensionSelector implements SingleValueDimensionVectorSelector
  {
    private final ReadableVectorInspector inspector;
    private final Object[] objects;
    private final int[] ids;
    private final Map<String, Integer> valueToId = new HashMap<>();
    private final List<String> idToValue = new ArrayList<>();
    private int vectorId = ReadableVectorInspector.NULL_ID;

    BatchStringDimensionSelector(final ReadableVectorInspector inspector, final Object[] objects)
    {
      this.inspector = inspector;
      this.objects = objects;
      this.ids = new int[inspector.getMaxVectorSize()];
    }

    @Override
    public int[] getRowVector()
    {
      if (vectorId != inspector.getId()) {
        for (int i = 0; i < inspector.getCurrentVectorSize(); i++) {
          final String value = (String) objects[i];
          ids[i] = valueToId.computeIfAbsent(
              value,
              ignored -> {
                idToValue.add(value);
                return idToValue.size() - 1;
              }
          );
        }
        vectorId = inspector.getId();
      }
      return ids;
    }

    @Override
    public int getValueCardinality()
    {
      return CARDINALITY_UNKNOWN;
    }

    @Nullable
    @Override
    public String lookupName(final int id)
    {
      return idToValue.get(id);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return null;
    }

    @Override
    public int getMaxVectorSize()
    {
      return inspector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return inspector.getCurrentVectorSize();
    }
  }

  private static class CapabilitiesInspector implements ColumnInspector
  {
    private final RowSignature signature;

    CapabilitiesInspector(final RowSignature signature)
    {
      this.signature = signature;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return getCapabilities(signature, column);
    }
  }
}
