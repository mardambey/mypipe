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

import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An abstract JUnit test for thoroughly testing a Repository implementation
 */
public abstract class AbstractTestRepository<R extends Repository> {
  private static final String SUB = "sub";
  private static final String CONF = "conf";
  private static final String NOCONF = "noconf";
  private static final String VALIDATING = "validating";
  private static final String FOO = "foo";
  private static final String BAR = "bar";
  private static final String BAZ = "baz";

  private R repo;

  protected abstract R createRepository();

  @Before
  public void setUpRepository() {
    repo = createRepository();
  }

  @After
  public void tearDownRepository() {
    repo = null;
  }

  protected final R getRepo() {
    return repo;
  }

  @Test
  public void testRepository() throws SchemaValidationException {
    // lookup a subject that does not exist, when none do
    Subject none = repo.lookup(SUB);
    Assert.assertNull("non-existent subject lookup should return null", none);
    // ensure that when there are no subjects, an empty iterable is produced
    Iterable<Subject> subjects = repo.subjects();
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
    Subject sub3 = repo.lookup(SUB);
    Assert.assertNotNull("subject lookup failed", sub3);
    Assert.assertEquals("subject lookup failed", sub.getName(), sub3.getName());
    
    // lookup a subject that does not exist, this time when some do
    Subject none2 = repo.lookup("nothing");
    Assert.assertNull("non-existent subject lookup should return null", none2);

    // go through all subjects
    boolean hasSub = false;
    for (Subject s : repo.subjects()) {
      if (sub.getName().equals(s.getName())) {
        hasSub = true;
        break;
      }
    }
    Assert.assertTrue("subjects() did not contain registered subject: " + sub,
        hasSub);
    
    // ensure that latest is null when a subject is first created
    SchemaEntry noLatest = sub.latest();
    Assert.assertNull("latest msut be null if it does not exist", noLatest);
    
    // ensure that registerIfLatest does not register if provided a latest
    // value when latest is null
    SchemaEntry didNotRegister = 
        sub.registerIfLatest(FOO, new SchemaEntry("not", "there"));
    Assert.assertNull(
        "registerIfLatest must return null if there are no schemas in the " +
        "subject and the passed in latest is not null. Found: " + 
            didNotRegister, didNotRegister);

    // ensure registerIflatest works when latest is null
    SchemaEntry foo = sub.registerIfLatest(FOO, null);
    validateSchemaEntry(FOO, foo);
    
    // ensure that register is idempotent
    SchemaEntry foo2 = sub.register(FOO);
    Assert.assertEquals("duplicate schema registration must be idempotent",
        foo, foo2);
    validateSchemaEntry(FOO, foo2);
    
    // ensure registerIflatest works when latest is not null
    SchemaEntry bar = sub.registerIfLatest(BAR, foo2);
    validateSchemaEntry(BAR, bar);
    
    // ensure registerIfLatest does not register if latest does not match
    SchemaEntry none3 = sub.registerIfLatest("none", foo);
    Assert.assertNull(
        "registerIfLatest must return null if latest does not match", none3);
    // ensure registerIfLatest does not register when provided null does not match
    SchemaEntry none4 = sub.registerIfLatest("none", null);
    Assert.assertNull(
        "registerIfLatest must return null if there is a latest schema in"
            + " the subject and the passed in value is null", none4);
    
    // ensure register can add new schemas
    SchemaEntry baz = sub.register(BAZ);
    validateSchemaEntry(BAZ, baz);

    // test lookup
    Subject subject = repo.lookup(SUB);
    Assert.assertNotNull("lookup of previously registered subject failed",
        subject);
    
    // test latest
    Assert.assertEquals("latest schema must match last registered", baz,
        subject.latest());
    boolean foundfoo = false, foundbar = false;
    for (SchemaEntry s : sub3.allEntries()) {
      if (s.equals(foo)) {
        foundfoo = true;
      }
      if (s.equals(bar)) {
        foundbar = true;
      }
    }
    Assert.assertTrue("AllEntries did not contain schema: " + FOO, foundfoo);
    Assert.assertTrue("AllEntries did not contain schema: " + BAR, foundbar);

    //ensure order of allEntries is correct
    Iterator<SchemaEntry> allEntries = sub3.allEntries().iterator();
    // latest must match first one:
    Assert.assertEquals("Latest must be first returned from allEntries()", 
        sub3.latest(), allEntries.next());
    Assert.assertEquals("second-latest must be BAR", bar, allEntries.next());
    Assert.assertEquals("third must be FOO", foo, allEntries.next());
    
    
    // test lookupBySchema
    SchemaEntry resultfoo = subject.lookupBySchema(foo.getSchema());
    Assert.assertEquals("lookup by Schema did not return same result", foo,
        resultfoo);
    SchemaEntry notThere = subject.lookupBySchema("notThere");
    Assert.assertNull("non existent schema should return null", notThere);
    // test lookupById
    SchemaEntry resultfooid = subject.lookupById(foo.getId());
    Assert.assertEquals("lookup by ID did not return same result", foo,
        resultfooid);
    SchemaEntry notTherById = subject.lookupById("notThere");
    Assert.assertNull("non existent schema should return null", notTherById);
    
    // test integralKeys() 
    if(subject.integralKeys()) {
      Integer.parseInt(resultfoo.getId());
    }

  }
  
  @Test
  public void testSubjectConfigs() {
    String testKey = "test.key";
    String testVal = "test.val";
    String testVal2 = "test.val2";
    SubjectConfig conf = new SubjectConfig.Builder().set(testKey, testVal).build();
    SubjectConfig conf2 = new SubjectConfig.Builder().set(testKey, testVal2).build();
    // lookup a subject that does not exist, when none do
    Subject none = repo.lookup(CONF);
    Assert.assertNull("non-existent subject lookup should return null", none);
    
    // register a subject
    Subject sub = repo.register(CONF, conf);
    Assert.assertNotNull("failed to register subject: " + CONF, sub);
    // a duplicate register is idempotent; the result is the same
    Subject sub2 = repo.register(CONF, conf2);
    Assert.assertNotNull("failed to re-register subject: " + SUB, sub2);
    validateSubject(sub, sub2);
    
    // lookup subject that was just registered
    Subject sub3 = repo.lookup(CONF);
    validateSubject(sub, sub3);
    
    // lookup something that is not there
    Subject sub4 = repo.lookup(NOCONF);
    Assert.assertNull("subject should not exist", sub4);
    
  }
  
  @Test
  public void testValidation() {
    SubjectConfig conf = new SubjectConfig.Builder().addValidator(ValidatorFactory.REJECT_VALIDATOR).build();
    // lookup a subject that does not exist, when none do
    Subject none = repo.lookup(VALIDATING);
    Assert.assertNull("non-existent subject lookup should return null", none);
    
    // register a subject that should reject all schemas
    Subject sub = repo.register(VALIDATING, conf);
    Assert.assertNotNull("failed to register subject: " + VALIDATING, sub);
    
    boolean threw = false;
    try {
      sub.register("stuff");
    } catch (SchemaValidationException e) {
      threw = true;
    }
    Assert.assertTrue("must throw a SchemaValidationException", threw);
  }

  private void validateSubject(Subject sub, Subject sub3) {
    Assert.assertEquals(
        "subject names do not match",
        sub.getName(), sub3.getName());
    Assert.assertEquals(
        "subject configurations do not match",
        sub.getConfig(), sub3.getConfig());
  }

  private void validateSchemaEntry(String expectedSchema, SchemaEntry foo) {
    Assert.assertNotNull("Failed to create SchemaEntry with schema: "
        + expectedSchema, foo);
    Assert.assertEquals("SchemaEntry does not have expected schema value",
        expectedSchema, foo.getSchema());
    Assert.assertNotNull("SchemaEntry does not have a valid id", foo.getId());
    Assert.assertFalse("SchemaEntry has an empty id", foo.getId().isEmpty());
  }

}
