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
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An unbounded in memory {@link SchemaEntryCache} that never evicts any values;
 */
public class InMemorySchemaEntryCache implements SchemaEntryCache {

  private final ConcurrentHashMap<String, SchemaEntry> schemaToEntry =
      new ConcurrentHashMap<String, SchemaEntry>();
  private final ConcurrentHashMap<String, SchemaEntry> idToSchema =
      new ConcurrentHashMap<String, SchemaEntry>();
  private final LinkedList<SchemaEntry> schemasInOrder = 
      new LinkedList<SchemaEntry>();

  @Override
  public SchemaEntry lookupBySchema(String schema) {
    return schemaToEntry.get(schema);
  }

  @Override
  public SchemaEntry lookupById(String id) {
    return idToSchema.get(id);
  }

  @Override
  public synchronized SchemaEntry add(SchemaEntry entry) {
    if (null == entry) {
      return entry;
    }
    SchemaEntry prior = schemaToEntry.putIfAbsent(entry.getSchema(), entry);
    if (null != prior) {
      entry = prior;
    }
    idToSchema.put(entry.getId(), entry);
    schemasInOrder.push(entry);
    return entry;
  }

  /** return all of the values in this cache **/
  public synchronized Iterable<SchemaEntry> values() {
    return new ArrayList<SchemaEntry>(schemasInOrder);
  }

  public static class Factory implements SchemaEntryCache.Factory {
    @Override
    public SchemaEntryCache createSchemaEntryCache() {
      return new InMemorySchemaEntryCache();
    }
  }

}
