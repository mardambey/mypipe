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

import java.util.concurrent.ConcurrentHashMap;

/**
 * An unbounded in memory {@link SubjectCache} that never evicts any values.
 */
public class InMemorySubjectCache implements SubjectCache {
  private final ConcurrentHashMap<String, Subject> subjects = 
      new ConcurrentHashMap<String, Subject>();

  @Override
  public Subject lookup(String name) {
    return subjects.get(name);
  }

  @Override
  public Subject add(Subject subject) {
    if (subject == null) {
      return subject;
    }
    Subject prior = subjects.putIfAbsent(subject.getName(), subject);
    return (null != prior) ? prior : subject;
  }

  /**
   * @return All fo the {@link Subject} values in this
   *         {@link InMemorySubjectCache}
   */
  public Iterable<Subject> values() {
    return subjects.values();
  }

}
