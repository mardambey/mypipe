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
 * {@link SchemaEntry} is composed of a schema and its corresponding id.
 */
public final class SchemaEntry {
  private final String id;
  private final String schema;

  /**
   * Primary constructor taking a literal id and schema.
   */
  public SchemaEntry(String id, String schema) {
    this.id = id;
    this.schema = schema;
  }

  /**
   * Constructor taking the string representation of the SchemaEntry produced by
   * {@link #toString()}
   */
  public SchemaEntry(String stringForm) {
    int tab = stringForm.indexOf('\t');
    if (tab < 1) {
      throw new IllegalArgumentException("Invalid Schema Entry Serialization: "
          + stringForm);
    }
    this.id = stringForm.substring(0, tab);
    this.schema = stringForm.substring(tab + 1);
  }

  /** @return the id */
  public String getId() {
    return id;
  }

  /** @return the schema */
  public String getSchema() {
    return schema;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SchemaEntry other = (SchemaEntry) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return id + "\t" + schema;
  }

}
