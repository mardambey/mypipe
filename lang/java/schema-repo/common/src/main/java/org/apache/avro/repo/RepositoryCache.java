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
 * <p>
 * A {@link RepositoryCache} composes a {@link SubjectCache} with a
 * {@link SchemaEntryCache.Factory}, using
 * {@link Subject#cacheWith(Subject, SchemaEntryCache)} to wrap {@link Subject}
 * instances prior to insertion into the {@link SubjectCache}.
 */
public class RepositoryCache implements SubjectCache {

  private final SubjectCache subjects;
  private final SchemaEntryCache.Factory entryCacheFactory;

  @Inject
  public RepositoryCache(SubjectCache subjects,
      SchemaEntryCache.Factory entryCacheFactory) {
    this.subjects = subjects;
    this.entryCacheFactory = entryCacheFactory;
  }

  @Override
  public Subject add(Subject entry) {
    return subjects.add(Subject.cacheWith(entry,
        entryCacheFactory.createSchemaEntryCache()));
  }

  @Override
  public Subject lookup(String name) {
    return subjects.lookup(name);
  }
}
