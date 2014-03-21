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

import javax.inject.Inject;


/**
 * A {@link Repository} that stores its data in memory and is not persistent.
 * This is useful primarily for testing.
 */
public class InMemoryRepository implements Repository {
  private final InMemorySubjectCache subjects = new InMemorySubjectCache();
  private final ValidatorFactory validators;
  
  @Inject
  public InMemoryRepository(ValidatorFactory validators) {
    this.validators = validators;
  }

  @Override
  public Subject register(String subjectName, SubjectConfig config) {
    return subjects.add(Subject.validatingSubject(new MemSubject(subjectName, config), validators));
  }

  @Override
  public Subject lookup(String subject) {
    return subjects.lookup(subject);
  }

  /**
   * List all subjects
   */
  @Override
  public Iterable<Subject> subjects() {
    return subjects.values();
  }

  private static class MemSubject extends Subject {
    private final InMemorySchemaEntryCache schemas = new InMemorySchemaEntryCache();
    private SchemaEntry latest = null;
    private int nextId = 0;
    private SubjectConfig config;

    protected MemSubject(String name, SubjectConfig config) {
      super(name);
      this.config = RepositoryUtil.safeConfig(config);
    }
    
    @Override
    public SubjectConfig getConfig() {
      return config;
    }

    @Override
    public synchronized SchemaEntry register(String schema)
        throws SchemaValidationException {
      String id = String.valueOf(nextId);
      SchemaEntry toRegister = new SchemaEntry(id, schema);
      SchemaEntry valueInCache = schemas.add(toRegister);
      if (toRegister == valueInCache) {
        // schema is new
        nextId++;
        this.latest = toRegister;
      }
      return valueInCache;
    }

    @Override
    public synchronized SchemaEntry registerIfLatest(String schema,
        SchemaEntry latest) throws SchemaValidationException {
      if (latest == this.latest
          || (latest != null && latest.equals(this.latest))) {
        return register(schema);
      } else {
        return null;
      }
    }

    @Override
    public SchemaEntry lookupBySchema(String schema) {
      return schemas.lookupBySchema(schema);
    }

    @Override
    public SchemaEntry lookupById(String id) {
      return schemas.lookupById(id);
    }

    @Override
    public synchronized SchemaEntry latest() {
      return latest;
    }

    @Override
    public synchronized Iterable<SchemaEntry> allEntries() {
      return schemas.values();
    }
    
    @Override
    public boolean integralKeys() {
      return true;
    }
  }

}
