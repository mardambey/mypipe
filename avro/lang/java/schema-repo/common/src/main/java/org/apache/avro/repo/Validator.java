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
 * A Validator may be bound to each {@link Subject} in a {@link Repository}
 * to ensure that schemas added to the {@link Subject} are mutualy compatible.
 * <br/>
 * <br/>
 * There are many useful notions of compatibility, for example:
 * <li>Forwards Compatible: A user of any old schema
 *     is able to read data written with the new schema</li>
 * <li>Backwards Compabible: A user of the most recent schema
 *     is able to read data written with any older schema</li>
 * <li>Full Compatibility: A user of any schema 
 *     is able to read data written in any other schema</li>
 * <li>N+1 Compatibility: Forward compatibility constrained to 
 *     only the verison one prior to the current version.  
 *     e.g. a reader with the second most recent schema 
 *     is able to read data written with the most recent schema</li>
 * <li>N+1 Compatibility: Backward compatibility constrained to
 *     only the verison one prior to the current version.
 *     e.g. a reader with the most recent schema
 *     is able to read data written with the second most recent schema</li>
 */
public interface Validator {

  /**
   * Validate that a schema is compatible with the schemas provided.
   * 
   * @param schemaToValidate
   *   The schema to validate.
   * @param schemasInOrder
   *   The schemas to validate against,
   *   presented in order from latest to oldest.
   * @throws SchemaValidationException
   *   if {@code schemaToValidate} is not compatible with the schemas
   *   in {@code schemasInOrder}
   */
  void validate(String schemaToValidate, Iterable<SchemaEntry> schemasInOrder)
      throws SchemaValidationException;
  
}
