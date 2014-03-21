/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.repo;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileRepository extends AbstractTestRepository<FileRepository> {
  private static final String TEST_PATH = "target/test/TestFileRepository-paths/";
  private static final String REPO_PATH = "target/test/TestFileRepository/";

  @BeforeClass
  public static void setup() {
    rmDir(new File(TEST_PATH));
    rmDir(new File(REPO_PATH));
  }

  @After
  public void cleanUp() throws IOException {
    getRepo().close();
  }

  @Override
  protected FileRepository createRepository() {
    return newRepo(REPO_PATH);
  }
  
  private FileRepository newRepo(String path) {
    return new FileRepository(path, new ValidatorFactory.Builder().build());
  }

  @Test
  public void testPathHandling() throws SchemaValidationException {
    String paths[] = new String[] {
        "data", "data/", "/tmp/file_repo",
        "/tmp/file_repo/", "/tmp/file_repo/" };

    for (String path : paths) {
      FileRepository r = newRepo(TEST_PATH + path);
      try {
        File expected = new File(TEST_PATH, path);
        assertTrue("Expected directory not created: " + 
        expected.getAbsolutePath() + " for path: " + path, expected.exists());
      } finally {
        r.close();
        // should be ok to call close twice
        r.close();
      }
    }
    // verify idempotent
    newRepo(TEST_PATH + "/tmp/repo").close();
    newRepo(TEST_PATH + "/tmp/repo").close();
  }

  @Test
  public void testReadWritten() throws SchemaValidationException {
    String path = TEST_PATH + "/readWrite";
    FileRepository r = newRepo(path);
    try {
      r.register("sub1", null).register("sc1");
      r.register("sub2", null).register("sc2");
      r.register("sub2", null).register("sc3");
    } finally {
      r.close();
    }
    r = newRepo(path);
    try {
      Subject s1 = r.lookup("sub1");
      Assert.assertNotNull(s1);
      Subject s2 = r.lookup("sub2");
      Assert.assertNotNull(s2);

      SchemaEntry e1 = s1.lookupBySchema("sc1");
      Assert.assertNotNull(e1);
      Assert.assertEquals("sc1", e1.getSchema());

      SchemaEntry e2 = s2.lookupBySchema("sc2");
      Assert.assertNotNull(e2);
      Assert.assertEquals("sc2", e2.getSchema());

      SchemaEntry e3 = s2.lookupBySchema("sc3");
      Assert.assertNotNull(e3);
      Assert.assertEquals("sc3", e3.getSchema());
    } finally {
      r.close();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidDir() throws IOException {
    String badPath = TEST_PATH + "/bad";
    new File(TEST_PATH).mkdirs();
    new File(badPath).createNewFile();
    FileRepository r = newRepo(badPath);
    r.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testCantUseClosedRepo() {
    FileRepository r = newRepo(TEST_PATH + "/tmp/repo");
    r.close();
    r.lookup("nothing");
  }

  private static void rmDir(File dir) {
    if (!dir.exists() || !dir.isDirectory()) {
      return;
    }
    for (String filename : dir.list()) {
      File entry = new File(dir, filename);
      if (entry.isDirectory()) {
        rmDir(entry);
      } else {
        entry.delete();
      }
    }
    dir.delete();
  }
}
