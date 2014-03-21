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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestReadOnlyRepository {
  private static final String SUB = "sub";
  private static final String FOO = "foo";

  private InMemoryRepository repo;
  private ReadOnlyRepository readOnlyRepo;

  @Before
  public void setUpRepository() {
    repo = new InMemoryRepository(new ValidatorFactory.Builder().build());
    readOnlyRepo = new ReadOnlyRepository(repo);
  }

  @After
  public void tearDownRepository() {
    repo = null;
    readOnlyRepo = null;
  }

  @Test
  public void testReadOnlyRepository() throws SchemaValidationException {
    
    // lookup a subject that does not exist, when none do
    Subject none = readOnlyRepo.lookup(SUB);
    Assert.assertNull("non-existent subject lookup should return null", none);
    // ensure that when there are no subjects, an empty iterable is produced
    Iterable<Subject> subjects = readOnlyRepo.subjects();
    Assert.assertNotNull("subjects must not be null");
    Assert.assertFalse("subjects must be empty", subjects.iterator().hasNext());
    
    // register a subject
    Subject sub = repo.register(SUB, null);
    Assert.assertNotNull("failed to register subject: " + SUB, sub);
    // a duplicate register is idempotent; the result is the same
    Subject sub2 = repo.register(SUB, null);
    Assert.assertNotNull("failed to re-register subject: " + SUB, sub2);
    Assert.assertEquals(
        "registering a subject twice did not produce the same result",
        sub.getName(), sub2.getName());

    // lookup subject that was just registered
    Subject readOnlySubject = readOnlyRepo.lookup(SUB);
    Assert.assertNotNull("subject lookup failed", readOnlySubject);
    Assert.assertEquals("subject lookup failed", sub.getName(), readOnlySubject.getName());
    
    // lookup a subject that does not exist, this time when some do
    Subject none2 = readOnlyRepo.lookup("nothing");
    Assert.assertNull("non-existent subject lookup should return null", none2);

    // go through all subjects
    boolean hasSub = false;
    for (Subject s : readOnlyRepo.subjects()) {
      if (sub.getName().equals(s.getName())) {
        hasSub = true;
        break;
      }
    }
    Assert.assertTrue("subjects() did not contain registered subject: " + sub,
        hasSub);

    SchemaEntry foo = sub.register(FOO);
    boolean foundfoo = false;
    for (SchemaEntry s : readOnlySubject.allEntries()) {
      if (s.equals(foo)) {
        foundfoo = true;
      }
    }
    Assert.assertTrue("AllEntries did not contain schema: " + FOO, foundfoo);

  }

  @Test(expected=IllegalStateException.class)
  public void testCannotCreateSubject() {
    readOnlyRepo.register(null, null);
  }

  @Test(expected=IllegalStateException.class)
  public void testCannotRegisterSchema() throws SchemaValidationException {
    repo.register(FOO, null);
    readOnlyRepo.lookup(FOO).register(null);
  }

  @Test(expected=IllegalStateException.class)
  public void testCannotRegisterSchemaIfLatest() throws SchemaValidationException {
    repo.register(FOO, null);
    readOnlyRepo.lookup(FOO).registerIfLatest(null, null);
  }

}
