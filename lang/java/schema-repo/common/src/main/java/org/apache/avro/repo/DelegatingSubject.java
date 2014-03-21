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


/**
 * A {@link DelegatingSubject} is a Subject that delegates work to an underlying
 * {@link Subject}.
 * 
 * Specific implementations may override various methods.
 * 
 */
public abstract class DelegatingSubject extends Subject {
  private final Subject delegate;

  /**
   * A {@link DelegatingSubject} delegates work to a provided Subject.
   **/
  protected DelegatingSubject(Subject delegate) {
    super(delegate.getName());
    this.delegate = delegate;
  }

  @Override
  public SchemaEntry register(String schema) throws SchemaValidationException {
    return delegate.register(schema);
  }

  @Override
  public SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
      throws SchemaValidationException {
    return delegate.registerIfLatest(schema, latest);
  }

  @Override
  public SchemaEntry lookupBySchema(String schema) {
    return delegate.lookupBySchema(schema);
  }

  @Override
  public SchemaEntry lookupById(String id) {
    return delegate.lookupById(id);
  }

  @Override
  public SchemaEntry latest() {
    return delegate.latest();
  }

  @Override
  public Iterable<SchemaEntry> allEntries() {
    return delegate.allEntries();
  }
  
  @Override
  public SubjectConfig getConfig() {
    return delegate.getConfig();
  }

  @Override
  public boolean integralKeys() {
    return delegate.integralKeys();
  }
}
