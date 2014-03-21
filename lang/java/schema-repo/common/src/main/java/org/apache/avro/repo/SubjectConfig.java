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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SubjectConfig} is effectively a Map<String, String> , with reserved
 * keys and default values for certain keys.
 * <br/>
 * Keys starting with "repo." are reserved.
 * 
 */
public class SubjectConfig {
  private static final SubjectConfig EMPTY = new Builder().build();
  
  private final Map<String, String> conf;
  private final Set<String> validators;

  private SubjectConfig(Map<String, String> conf, Set<String> validators) {
    this.conf = conf;
    this.validators = validators;
  }

  public String get(String key) {
    return conf.get(key);
  }
  
  public Set<String> getValidators() {
    return validators;
  }
  
  public Map<String, String> asMap() {
    return conf;
  }
  
  public static SubjectConfig emptyConfig() {
    return EMPTY;
  }
  
  @Override
  public int hashCode() {
    return conf.hashCode() * 31 + validators.hashCode();
   }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubjectConfig other = (SubjectConfig) obj;
    if (!validators.equals(other.validators))
      return false;
    if (!conf.equals(other.conf))
      return false;
    return true;
  }

  public static class Builder {
    private static final String RESERVED_PREFIX = "repo.";
    private static final String VALIDATORS_KEY = "repo.validators";
    
    private final HashMap<String, String> conf = new HashMap<String, String>();
    private final HashSet<String> validators = new HashSet<String>();
    
    public Builder set(Map<String, String> config) {
      for(Map.Entry<String, String> entry : config.entrySet()) {
        set(entry.getKey(), entry.getValue());
      }
      return this;
    }
    
    public Builder set(String key, String value) {
      if(key.startsWith(RESERVED_PREFIX)) {
        if(VALIDATORS_KEY.equals(key)) {
          setValidators(commaSplit(value));
        } else {
          throw new RuntimeException("SubjectConfig keys starting with '" +
              RESERVED_PREFIX + "' are reserved, failed to set: " + key + 
              " to value: " + value);
        }
      } else {
        conf.put(key, value);
      }
      return this;
    }
    
    public Builder setValidators(Collection<String> validatorNames) {
      this.validators.clear();
      this.conf.remove(VALIDATORS_KEY);
      if(!validatorNames.isEmpty()) {
        this.validators.addAll(validatorNames);
        this.conf.put(VALIDATORS_KEY, commaJoin(validators));
      }
      return this;
    }
    
    public Builder addValidator(String validatorName) {
      this.validators.add(validatorName);
      this.conf.put(VALIDATORS_KEY, commaJoin(validators));
      return this;
    }
        
    public SubjectConfig build() {
      return new SubjectConfig(
          Collections.unmodifiableMap(new HashMap<String, String>(conf)),
          Collections.unmodifiableSet(new HashSet<String>(validators)));
    }

  }
  
  /**
   * Helper method for splitting a string by a delimiter with 
   * java.util.String.split().
   * Omits empty values.
   * @param toSplit The string to split.  If null, an empty 
   *   String[] is returned
   * @return A String[] containing the non-empty values resulting 
   *   from the split.  Does not return null.
   */
  private static List<String> commaSplit(String toSplit) {
    if (toSplit == null) {
      return Collections.emptyList();
    }
    ArrayList<String> list = new ArrayList<String>();
    for(String s : toSplit.split(",")) {
      s = s.trim();
      if (!s.isEmpty()) {
        list.add(s);
      }
    }
    return list;
  }
  
  private static String commaJoin(Collection<String> strings) {
    StringBuilder sb = new StringBuilder();
    for(String s : strings) {
      sb.append(s).append(',');
    }
    return sb.toString();
  }
  
}
