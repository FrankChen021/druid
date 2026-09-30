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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Reserves one lane's progress pages while delegating optional growth to the query-wide lease. */
class LaneMergeMemoryLease implements MergeMemoryLease
{
  private final MergeMemoryLease queryLease;
  private final Deque<MergeMemoryPage> guaranteedAvailable = new ArrayDeque<>();
  private final Set<MergeMemoryPage> guaranteedPages = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<MergeMemoryPage> borrowedPages = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<PageHandle, MergeMemoryPage> handles = new IdentityHashMap<>();
  private boolean closed;

  LaneMergeMemoryLease(final MergeMemoryLease queryLease, final int guaranteedPageCount)
  {
    this.queryLease = queryLease;
    final List<MergeMemoryPage> pages = queryLease.tryAcquirePages(guaranteedPageCount).orElseThrow(
        () -> new ISE("Unable to assign [%d] guaranteed pages to an aggregation lane", guaranteedPageCount)
    );
    guaranteedPages.addAll(pages);
    guaranteedAvailable.addAll(pages);
  }

  @Override
  public synchronized Optional<List<MergeMemoryPage>> tryAcquirePages(final int count)
  {
    if (closed) {
      throw new ISE("Lane merge-memory lease is closed");
    }
    if (count <= 0) {
      throw new IAE("Page count[%d] must be positive", count);
    }
    final int additionalPages = Math.max(0, count - guaranteedAvailable.size());
    final List<MergeMemoryPage> borrowed;
    if (additionalPages == 0) {
      borrowed = Collections.emptyList();
    } else {
      final Optional<List<MergeMemoryPage>> acquired = queryLease.tryAcquirePages(additionalPages);
      if (!acquired.isPresent()) {
        return Optional.empty();
      }
      borrowed = acquired.get();
      borrowedPages.addAll(borrowed);
    }

    final List<MergeMemoryPage> result = new ArrayList<>(count);
    for (int i = 0; i < count - borrowed.size(); i++) {
      result.add(newHandle(guaranteedAvailable.removeFirst()));
    }
    borrowed.forEach(page -> result.add(newHandle(page)));
    return Optional.of(result);
  }

  @Override
  public int pageSize()
  {
    return queryLease.pageSize();
  }

  @Override
  public synchronized void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    handles.clear();
    final List<MergeMemoryPage> pages = new ArrayList<>(guaranteedPages.size() + borrowedPages.size());
    pages.addAll(guaranteedPages);
    pages.addAll(borrowedPages);
    guaranteedAvailable.clear();
    guaranteedPages.clear();
    borrowedPages.clear();
    pages.forEach(MergeMemoryPage::close);
  }

  private PageHandle newHandle(final MergeMemoryPage page)
  {
    final PageHandle handle = new PageHandle();
    handles.put(handle, page);
    return handle;
  }

  private synchronized ByteBuffer get(final PageHandle handle)
  {
    final MergeMemoryPage page = handles.get(handle);
    if (closed || page == null) {
      throw new ISE("Lane merge-memory page handle is closed");
    }
    return page.get();
  }

  private synchronized void release(final PageHandle handle)
  {
    final MergeMemoryPage page = handles.remove(handle);
    if (page == null) {
      return;
    }
    if (guaranteedPages.contains(page)) {
      guaranteedAvailable.addLast(page);
    } else if (borrowedPages.remove(page)) {
      page.close();
    }
  }

  private class PageHandle implements MergeMemoryPage
  {
    @Override
    public ByteBuffer get()
    {
      return LaneMergeMemoryLease.this.get(this);
    }

    @Override
    public void close()
    {
      release(this);
    }
  }
}
