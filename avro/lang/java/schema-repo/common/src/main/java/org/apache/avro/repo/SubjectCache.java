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
 * <p>
 * A {@link SubjectCache} is a cache from a string subject name to a
 * {@link Subject}
 * </p>
 * <p>
 * In a {@link Repository} subjects can be cached because they can only be
 * created. However, a {@link Subject} can have its meta-data altered, so this
 * cannot be cached.
 * </p>
 * {@link #add(Subject)} and {@link #lookup(String)} must be thread-safe
 * with respect to each-other.
 */
public interface SubjectCache {
  /**
   * Look up a {@link Subject} by its name. Thread-safe.
   * 
   * @throws NullPointerException
   *           if the provided name is null
   */
  Subject lookup(String name);

  /**
   * Add or update the {@link Subject} entry in this cache.
   * 
   * @return the {@link Subject} that is in the cache after the call completes.
   *         If the entry already exists this is the pre-existing value,
   *         otherwise it is the value provided. If the value provided is null,
   *         returns null. Thread-safe.
   */
  Subject add(Subject entry);

}
