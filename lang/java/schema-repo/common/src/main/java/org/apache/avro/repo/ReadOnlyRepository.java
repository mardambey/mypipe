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

import java.util.ArrayList;

import javax.inject.Inject;

/**
 * ReadOnlyRepository is a {@link Repository} implementation that wraps another
 * {@link Repository} and rejects all operations that can modify the state of
 * the {@link Repository}.<br/>
 * <br/>
 * 
 * {@link #register(String, Map)}, {@link Subject#register(String)} 
 * and {@link Subject#registerIfLatest(String, SchemaEntry)} throw
 * {@link IllegalStateException} if called.
 */
public class ReadOnlyRepository implements Repository {
  private final Repository repo;
  
  /**
   * Create a repository that disallows mutations to the underlying repository.
   * @param repo The repository to wrap
   */
  @Inject
  public ReadOnlyRepository(Repository repo) {
    this.repo = repo;
  }

  @Override
  public Subject register(String subjectName, SubjectConfig config) {
    throw new IllegalStateException(
        "Cannot register a Subject in a ReadOnlyRepository");
  }

  @Override
  public Subject lookup(String subjectName) {
    return Subject.readOnly(repo.lookup(subjectName));
  }

  @Override
  public Iterable<Subject> subjects() {
    ArrayList<Subject> subjects = new ArrayList<Subject>();
    for(Subject sub : repo.subjects()) {
      subjects.add(Subject.readOnly(sub));
    }
    return subjects;
  }
}
