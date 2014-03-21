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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * A factory for mapping Validator names to instantiated instances. Validator
 * names starting with "repo."
 */
public class ValidatorFactory {
  public static final String REJECT_VALIDATOR = "repo.reject";

  private final HashMap<String, Validator> validators;

  private ValidatorFactory(HashMap<String, Validator> validators) {
    this.validators = validators;
  }

  /**
   * @param validatorNames
   *          The set of {@link Validator} names to resolve. Must not be null.
   * @return A list of {@link Validator}s. Not null.
   */
  public final List<Validator> getValidators(Set<String> validatorNames) {
    ArrayList<Validator> result = new ArrayList<Validator>();
    for (String name : validatorNames) {
      Validator v = validators.get(name);
      if (v != null) {
        result.add(v);
      }
    }
    return result;
  }

  public static class Builder {
    private final HashMap<String, Validator> validators;
    {
      validators = new HashMap<String, Validator>();
      validators.put(REJECT_VALIDATOR, new Reject());
    }

    /**
     * Configure this builder to return a {@link ValidatorFactory} that maps the
     * {@link Validator} provided to the name given. <br/>
     * The name must not be null and must not start with "repo.".
     */
    public Builder setValidator(String name, Validator validator) {
      if (name.startsWith("repo.")) {
        throw new RuntimeException("Validator names starting with 'repo.'"
            + " are reserved.  Attempted to set validator with name: " + name);
      }
      validators.put(name, validator);
      return this;
    }

    public ValidatorFactory build() {
      return new ValidatorFactory(new HashMap<String, Validator>(validators));
    }
  }

  private static class Reject implements Validator {
    @Override
    public void validate(String schemaToValidate,
        Iterable<SchemaEntry> schemasInOrder) throws SchemaValidationException {
      throw new SchemaValidationException(
          "repo.validator.reject validator always rejects validation");
    }
  }
}
