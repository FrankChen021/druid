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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;

import java.util.Optional;

/// Keeps final stage frame files available to the controller's leased remote-frame session.
public class LiveFramesMSQDestination implements MSQDestination
{
  public static final String TYPE = "liveFrames";
  public static final LiveFramesMSQDestination INSTANCE = new LiveFramesMSQDestination();

  private LiveFramesMSQDestination()
  {
    // Singleton.
  }

  @JsonCreator
  public static LiveFramesMSQDestination instance()
  {
    return INSTANCE;
  }

  @Override
  public ShuffleSpecFactory getShuffleSpecFactory(final int targetSize)
  {
    return ShuffleSpecFactories.globalSortWithTargetSize(targetSize);
  }

  @Override
  public Optional<Resource> getDestinationResource()
  {
    return Optional.of(new Resource(MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT, ResourceType.DATASOURCE));
  }

  @Override
  public long getRowsInTaskReport()
  {
    return 0;
  }

  @Override
  public MSQSelectDestination toSelectDestination()
  {
    return MSQSelectDestination.LIVEFRAMES;
  }
}
