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

import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;


public class TestValidatorFactory {
 
  @Test
  public void test() throws SchemaValidationException {
    Validator foo = new Validator() {
      @Override
      public void validate(String schemaToValidate,
          Iterable<SchemaEntry> schemasInOrder)
          throws SchemaValidationException {
      }
    };
    
    ValidatorFactory fact = new ValidatorFactory.Builder().setValidator("foo", foo).build();
    HashSet<String> fooset = new HashSet<String>();
    fooset.add("foo");
    fooset.add(null); // should ignore
    Assert.assertSame(foo, fact.getValidators(fooset).get(0));
    fact.getValidators(fooset).get(0).validate(null, null);
  }
  
  @Test(expected=RuntimeException.class)
  public void testInvalidName() {
    new ValidatorFactory.Builder()
      .setValidator("repo.willBreak", null);
  }
  
}
