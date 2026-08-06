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

package org.apache.druid.testing.junit5;

import org.apache.druid.java.util.common.FileUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Jupiter extension providing the file-oriented API used by Druid's shared test fixtures.
 * Explicit lifecycle methods are retained for fixtures invoked by tests in dependent modules.
 */
public class TempDirExtension implements BeforeEachCallback, AfterEachCallback
{
  private File root;

  @Override
  public void beforeEach(final ExtensionContext context)
  {
    create();
  }

  @Override
  public void afterEach(final ExtensionContext context)
  {
    delete();
  }

  public void create()
  {
    if (root == null) {
      root = FileUtils.createTempDir("druid-junit5");
    }
  }

  public void delete()
  {
    if (root != null) {
      try {
        FileUtils.deleteDirectory(root);
        root = null;
      }
      catch (IOException e) {
        throw new RuntimeException("Could not delete temporary directory " + root, e);
      }
    }
  }

  public File getRoot()
  {
    ensureCreated();
    return root;
  }

  public File newFolder(final String... folderNames) throws IOException
  {
    ensureCreated();
    if (folderNames.length == 0) {
      return Files.createTempDirectory(root.toPath(), "folder-").toFile();
    }
    File folder = root;
    for (final String folderName : folderNames) {
      folder = new File(folder, folderName);
    }
    if (!folder.mkdirs()) {
      throw new IOException("Could not create folder " + folder);
    }
    return folder;
  }

  public File newFile() throws IOException
  {
    ensureCreated();
    return Files.createTempFile(root.toPath(), "junit5-", null).toFile();
  }

  public File newFile(final String fileName) throws IOException
  {
    ensureCreated();
    final File file = new File(root, fileName);
    if (!file.createNewFile()) {
      throw new IOException("Could not create file " + file);
    }
    return file;
  }

  private void ensureCreated()
  {
    if (root == null) {
      throw new IllegalStateException("Temporary folder has not been created");
    }
  }
}
