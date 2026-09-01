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

package org.apache.druid.query;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.ResidentCursorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Internal datasource that transposes authorized rows into reusable, query-local column batches.
 * It deliberately has no persistent cache. Unsupported cursor shapes fall back to row-based processing.
 */
public class BatchedInlineDataSource extends LeafDataSource
{
  static final int BATCH_SIZE = 1_024;

  private final Iterable<Object[]> rows;
  private final RowSignature signature;

  public BatchedInlineDataSource(final Iterable<Object[]> rows, final RowSignature signature)
  {
    this.rows = rows;
    this.signature = signature;
  }

  public Iterable<Object[]> getRows()
  {
    return rows;
  }

  public RowSignature getRowSignature()
  {
    return signature;
  }

  @Override
  public Set<String> getTableNames()
  {
    return Set.of();
  }

  @Override
  public boolean isCacheable(final boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public boolean isProcessable()
  {
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
  }

  public static class Wrangler implements SegmentWrangler
  {
    @Override
    public Iterable<Segment> getSegmentsForIntervals(
        final DataSource dataSource,
        final Iterable<Interval> intervals
    )
    {
      return List.of(new BatchedInlineSegment((BatchedInlineDataSource) dataSource));
    }
  }

  private static class BatchedInlineSegment implements Segment
  {
    private final BatchedInlineDataSource dataSource;

    BatchedInlineSegment(final BatchedInlineDataSource dataSource)
    {
      this.dataSource = dataSource;
    }

    @Nullable
    @Override
    public SegmentId getId()
    {
      return null;
    }

    @Override
    public Interval getDataInterval()
    {
      return Intervals.ETERNITY;
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull final Class<T> clazz)
    {
      if (CursorFactory.class.equals(clazz)) {
        return clazz.cast(new BatchedCursorFactory(dataSource.rows, dataSource.signature));
      }
      return null;
    }

    @Override
    public void close()
    {
      // Nothing to close.
    }
  }

  private static class BatchedCursorFactory implements ResidentCursorFactory
  {
    private final Iterable<Object[]> rows;
    private final RowSignature signature;
    private final RowBasedCursorFactory<Object[]> rowCursorFactory;

    BatchedCursorFactory(final Iterable<Object[]> rows, final RowSignature signature)
    {
      this.rows = rows;
      this.signature = signature;
      final RowAdapter<Object[]> rowAdapter = column -> {
        final int columnNumber = signature.indexOf(column);
        return row -> columnNumber < 0 ? null : row[columnNumber];
      };
      this.rowCursorFactory = new RowBasedCursorFactory<>(Sequences.simple(rows), rowAdapter, signature);
    }

    @Override
    public CursorHolder makeCursorHolder(final CursorBuildSpec spec)
    {
      final CursorHolder rowCursorHolder = rowCursorFactory.makeCursorHolder(spec);
      return new CursorHolder()
      {
        @Nullable
        @Override
        public Cursor asCursor()
        {
          return rowCursorHolder.asCursor();
        }

        @Override
        public boolean canVectorize()
        {
          if (spec.getFilter() != null || !supportsVectorTypes(signature)) {
            return false;
          }
          final VirtualColumns virtualColumns = spec.getVirtualColumns();
          final ColumnCapabilitiesInspector inspector = new ColumnCapabilitiesInspector(signature);
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

        @Nullable
        @Override
        public VectorCursor asVectorCursor()
        {
          if (!canVectorize()) {
            throw new ISE("Batched inline cursor cannot vectorize this query");
          }
          return new BatchVectorCursor(rows, signature, spec);
        }

        @Override
        public List<OrderBy> getOrdering()
        {
          return rowCursorHolder.getOrdering();
        }

        @Override
        public void close()
        {
          rowCursorHolder.close();
        }
      };
    }

    @Override
    public RowSignature getRowSignature()
    {
      return signature;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return capabilities(signature, column);
    }
  }

  private static class BatchVectorCursor implements VectorCursor
  {
    private final BatchOffset offset;
    private final VectorColumnSelectorFactory selectorFactory;

    BatchVectorCursor(
        final Iterable<Object[]> rows,
        final RowSignature signature,
        final CursorBuildSpec spec
    )
    {
      this.offset = new BatchOffset(rows, signature);
      this.selectorFactory = new BatchVectorColumnSelectorFactory(offset, signature, spec.getVirtualColumns());
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
      return BATCH_SIZE;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }
  }

  private static class BatchOffset implements ReadableVectorOffset
  {
    private final Iterable<Object[]> rows;
    private final RowSignature signature;
    private final long[][] longColumns;
    private final Object[][] objectColumns;
    private final boolean[][] nullColumns;

    private Iterator<Object[]> iterator;
    private int currentSize;
    private int id;

    BatchOffset(final Iterable<Object[]> rows, final RowSignature signature)
    {
      this.rows = rows;
      this.signature = signature;
      this.longColumns = new long[signature.size()][];
      this.objectColumns = new Object[signature.size()][];
      this.nullColumns = new boolean[signature.size()][];
      for (int i = 0; i < signature.size(); i++) {
        final ColumnType type = signature.getColumnType(i).orElse(null);
        if (ColumnType.LONG.equals(type)) {
          longColumns[i] = new long[BATCH_SIZE];
          nullColumns[i] = new boolean[BATCH_SIZE];
        } else {
          objectColumns[i] = new Object[BATCH_SIZE];
        }
      }
      reset();
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
      loadBatch();
    }

    boolean isDone()
    {
      return currentSize == 0;
    }

    void reset()
    {
      iterator = rows.iterator();
      id = 0;
      loadBatch();
    }

    private void loadBatch()
    {
      int rowNumber = 0;
      while (rowNumber < BATCH_SIZE && iterator.hasNext()) {
        final Object[] row = iterator.next();
        for (int column = 0; column < signature.size(); column++) {
          if (longColumns[column] != null) {
            final Object value = row[column];
            nullColumns[column][rowNumber] = value == null;
            longColumns[column][rowNumber] = value == null ? 0L : ((Number) value).longValue();
          } else {
            objectColumns[column][rowNumber] = row[column];
          }
        }
        rowNumber++;
      }
      currentSize = rowNumber;
      id++;
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
      return BATCH_SIZE;
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
      throw new UnsupportedOperationException("contiguous batch");
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
          selector = virtualColumns.makeSingleValueDimensionVectorSelector(
              dimensionSpec,
              this,
              null,
              offset
          );
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
      throw new UnsupportedOperationException("Batched inline cursors do not support multi-value dimensions");
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
      return virtualColumns.getColumnCapabilitiesWithFallback(
          new ColumnCapabilitiesInspector(signature),
          column
      );
    }
  }

  private static class BatchLongVectorValueSelector implements VectorValueSelector
  {
    private final ReadableVectorInspector inspector;
    private final long[] longs;
    private final boolean[] nulls;
    private final float[] floats = new float[BATCH_SIZE];
    private final double[] doubles = new double[BATCH_SIZE];
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

  private static class BatchStringDimensionSelector implements SingleValueDimensionVectorSelector
  {
    private final ReadableVectorInspector inspector;
    private final Object[] objects;
    private final int[] ids = new int[BATCH_SIZE];
    private final Map<String, Integer> valueToId = new HashMap<>();
    private final List<String> idToValue = new ArrayList<>();
    private int vectorId = ReadableVectorInspector.NULL_ID;

    BatchStringDimensionSelector(final ReadableVectorInspector inspector, final Object[] objects)
    {
      this.inspector = inspector;
      this.objects = objects;
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

  private static class ColumnCapabilitiesInspector implements org.apache.druid.segment.ColumnInspector
  {
    private final RowSignature signature;

    ColumnCapabilitiesInspector(final RowSignature signature)
    {
      this.signature = signature;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return capabilities(signature, column);
    }
  }

  private static boolean supportsVectorTypes(final RowSignature signature)
  {
    for (int i = 0; i < signature.size(); i++) {
      final ColumnType type = signature.getColumnType(i).orElse(null);
      if (!ColumnType.LONG.equals(type) && !ColumnType.STRING.equals(type)) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static ColumnCapabilities capabilities(final RowSignature signature, final String column)
  {
    final ColumnType type = signature.getColumnType(column).orElse(null);
    if (type == null) {
      return null;
    }
    if (type.is(ValueType.STRING)) {
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(ColumnType.STRING)
                                   .setDictionaryEncoded(true)
                                   .setDictionaryValuesSorted(false)
                                   .setDictionaryValuesUnique(true)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    }
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type).setHasNulls(true);
  }
}
