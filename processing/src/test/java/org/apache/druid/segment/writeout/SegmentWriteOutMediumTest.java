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

package org.apache.druid.segment.writeout;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.testing.JupiterAssertions;
import org.apache.druid.testing.TempDirExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class SegmentWriteOutMediumTest
{
  public static Iterable<?> constructorFeeder()
  {
    return ImmutableList.of(
        TmpFileSegmentWriteOutMediumFactory.instance(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        OnHeapMemorySegmentWriteOutMediumFactory.instance()
    );
  }

  @RegisterExtension
  public TempDirExtension temporaryFolder = new TempDirExtension();

  private SegmentWriteOutMediumFactory factory;
  private SegmentWriteOutMedium medium;

  public SegmentWriteOutMediumTest(SegmentWriteOutMediumFactory factory)
  {
    this.factory = factory;
  }

  @BeforeEach
  public void setup() throws IOException
  {
    this.medium = factory.makeSegmentWriteOutMedium(temporaryFolder.newFolder());
  }

  @Test
  public void testSanity() throws IOException
  {
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    JupiterAssertions.assertTrue(bytes1.isOpen());
    JupiterAssertions.assertTrue(bytes2.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    JupiterAssertions.assertFalse(bytes1.isOpen());
    JupiterAssertions.assertFalse(bytes2.isOpen());
  }

  @Test
  public void testChildCloseFreesResourcesButNotParents() throws IOException
  {
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    JupiterAssertions.assertTrue(bytes1.isOpen());
    JupiterAssertions.assertTrue(bytes2.isOpen());

    SegmentWriteOutMedium childMedium = medium.makeChildWriteOutMedium();
    JupiterAssertions.assertTrue(childMedium.getClass().equals(medium.getClass()));

    WriteOutBytes bytes3 = childMedium.makeWriteOutBytes();
    WriteOutBytes bytes4 = childMedium.makeWriteOutBytes();

    JupiterAssertions.assertTrue(bytes3.isOpen());
    JupiterAssertions.assertTrue(bytes4.isOpen());

    Closer childCloser = childMedium.getCloser();
    childCloser.close();

    JupiterAssertions.assertFalse(bytes3.isOpen());
    JupiterAssertions.assertFalse(bytes4.isOpen());

    JupiterAssertions.assertTrue(bytes1.isOpen());
    JupiterAssertions.assertTrue(bytes2.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    JupiterAssertions.assertFalse(bytes1.isOpen());
    JupiterAssertions.assertFalse(bytes2.isOpen());
  }

  @Test
  public void testChildNotClosedExplicitlyIsClosedByParent() throws IOException
  {
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    JupiterAssertions.assertTrue(bytes1.isOpen());
    JupiterAssertions.assertTrue(bytes2.isOpen());

    SegmentWriteOutMedium childMedium = medium.makeChildWriteOutMedium();
    JupiterAssertions.assertTrue(childMedium.getClass().equals(medium.getClass()));

    WriteOutBytes bytes3 = childMedium.makeWriteOutBytes();
    WriteOutBytes bytes4 = childMedium.makeWriteOutBytes();

    JupiterAssertions.assertTrue(bytes3.isOpen());
    JupiterAssertions.assertTrue(bytes4.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    JupiterAssertions.assertFalse(bytes1.isOpen());
    JupiterAssertions.assertFalse(bytes2.isOpen());

    JupiterAssertions.assertFalse(bytes3.isOpen());
    JupiterAssertions.assertFalse(bytes4.isOpen());
  }
}
