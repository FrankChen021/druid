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
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.ResidentCursorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Reads the rows of a system table from its {@link SystemTableDataProvider}.
 *
 * <p>Each cursor passes the provider the conjuncts of the query filter that read only the provider's
 * {@link SystemTableDataProvider#getPushdownColumns() pushdown columns}, keeps the rows the caller may read according
 * to its {@link SystemTableAccess}, and then applies the whole query filter and the query's virtual columns. Rows are
 * authorized before any virtual column is computed, so a virtual column can't change which rows the caller sees.</p>
 *
 * <p>Queries without a filter whose columns, virtual columns and aggregators can be vectorized read the rows in
 * batches through {@link SystemTableVectorCursor}.</p>
 */
public class SystemTableCursorFactory implements ResidentCursorFactory
{
  private final SystemTableDataProvider provider;
  private final RowSignature rowSignature;
  private final SystemTableAccess access;
  private final RowAdapter<Object[]> rowAdapter;
  private final RowBasedCursorFactory<Object[]> capabilitiesCursorFactory;

  public SystemTableCursorFactory(
      final SystemTableDataProvider provider,
      final RowSignature rowSignature,
      final SystemTableAccess access
  )
  {
    this.provider = provider;
    this.rowSignature = rowSignature;
    this.access = access;
    this.rowAdapter = columnName -> {
      final int columnNumber = rowSignature.indexOf(columnName);
      return columnNumber >= 0 ? row -> row[columnNumber] : row -> null;
    };
    // Row-based cursors report the same capabilities whatever their rows.
    this.capabilitiesCursorFactory = new RowBasedCursorFactory<>(Sequences.empty(), rowAdapter, rowSignature);
  }

  @Override
  public CursorHolder makeCursorHolder(final CursorBuildSpec spec)
  {
    final Iterable<Object[]> providerRows = provider.getRows(getPushdownFilters(spec), spec.getPhysicalColumns());
    final Supplier<Sequence<Object[]>> rows = () -> makeAuthorizedRowSequence(providerRows, access);
    // Row-based cursor holders read their rows only once a cursor is made, so this reads nothing yet.
    final CursorHolder rowCursorHolder =
        new RowBasedCursorFactory<>(rows.get(), rowAdapter, rowSignature).makeCursorHolder(spec);
    final Closer closer = Closer.create();
    closer.register(rowCursorHolder);
    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return rowCursorHolder.asCursor();
      }

      @Override
      public boolean canVectorize()
      {
        return SystemTableVectorCursor.canVectorize(spec, rowSignature);
      }

      @Override
      public VectorCursor asVectorCursor()
      {
        if (!canVectorize()) {
          throw DruidException.defensive("This query on a system table can't be vectorized");
        }
        return closer.register(new SystemTableVectorCursor(rows, rowSignature, spec));
      }

      @Nullable
      @Override
      public List<OrderBy> getOrdering()
      {
        return rowCursorHolder.getOrdering();
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
  }

  /**
   * Returns the provider's rows that the caller may read, as a sequence that closes the rows' iterator, if it's
   * {@link Closeable}, once the sequence is closed or fully read, even if reading stopped early or failed.
   */
  static Sequence<Object[]> makeAuthorizedRowSequence(final Iterable<Object[]> rows, final SystemTableAccess access)
  {
    final Sequence<Object[]> rowSequence = new BaseSequence<>(
        new BaseSequence.IteratorMaker<Object[], Iterator<Object[]>>()
        {
          @Override
          public Iterator<Object[]> make()
          {
            return rows.iterator();
          }

          @Override
          public void cleanup(final Iterator<Object[]> iterator)
          {
            if (iterator instanceof Closeable closeable) {
              CloseableUtils.closeAndWrapExceptions(closeable);
            }
          }
        }
    );
    return access.rowPredicate() == null ? rowSequence : rowSequence.filter(access.rowPredicate()::test);
  }

  /**
   * Reads the columns of a row of the table by name.
   */
  public RowAdapter<Object[]> getRowAdapter()
  {
    return rowAdapter;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    final ColumnCapabilities capabilities = capabilitiesCursorFactory.getColumnCapabilities(column);
    if (capabilities != null && capabilities.is(ValueType.STRING)) {
      // String columns of system tables hold single values, which lets vectorized engines group by them.
      return ColumnCapabilitiesImpl.copyOf(capabilities).setHasMultipleValues(false);
    }
    return capabilities;
  }

  /**
   * Returns the conjuncts of the query filter that read only pushdown columns. A column that a virtual column of the
   * query replaces isn't the provider's column, so filters on it aren't pushed down.
   */
  private List<Filter> getPushdownFilters(final CursorBuildSpec spec)
  {
    final Set<String> pushdownColumns = provider.getPushdownColumns();
    final Filter filter = spec.getFilter();
    if (pushdownColumns.isEmpty() || filter == null) {
      return List.of();
    }

    final Collection<Filter> conjuncts = filter instanceof AndFilter andFilter
                                         ? andFilter.getFilters()
                                         : List.of(filter);
    final VirtualColumns virtualColumns = spec.getVirtualColumns();
    final List<Filter> pushdownFilters = new ArrayList<>();
    for (final Filter conjunct : conjuncts) {
      final Set<String> requiredColumns = conjunct.getRequiredColumns();
      if (!requiredColumns.isEmpty()
          && pushdownColumns.containsAll(requiredColumns)
          && requiredColumns.stream().noneMatch(virtualColumns::exists)) {
        pushdownFilters.add(conjunct);
      }
    }
    return pushdownFilters;
  }
}
