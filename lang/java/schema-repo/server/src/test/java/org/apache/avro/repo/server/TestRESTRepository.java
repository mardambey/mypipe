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

package org.apache.avro.repo.server;

import org.apache.avro.repo.InMemoryRepository;
import org.apache.avro.repo.ValidatorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.NotFoundException;

public class TestRESTRepository {
  RESTRepository repo;
  
  @Before
  public void setUp() {
    repo = new RESTRepository(new InMemoryRepository(new ValidatorFactory.Builder().build()));
  }
  
  @After
  public void tearDown() {
    repo = null;
  }
  
  @Test(expected=NotFoundException.class)
  public void testNonExistentSubjectList() throws Exception {
    repo.subjectList("nothing");
  }
  
  @Test(expected=NotFoundException.class)
  public void testNonExistentSubjectGetConfig() throws Exception {
    repo.subjectConfig("nothing");
  }

  @Test
  public void testCreateNullSubject() {
    Assert.assertEquals(400, repo.createSubject(null, null).getStatus());
  }
}