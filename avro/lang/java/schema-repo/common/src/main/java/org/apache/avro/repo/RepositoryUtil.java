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
import java.util.Properties;

/**
 * {@link RepositoryUtil} contains static helper methods for the
 * org.apache.avro.repo package.
 * <p>
 * {@link #subjectsToString(Iterable)} and
 * {@link #subjectNamesFromString(String)} can be used to encode
 * subjects to string format. <br/>
 * {@link #schemasToString(Iterable)} and {@link #schemasFromString(String)} can
 * be used to encode schemas to string format.
 * </p>
 * <p>
 * These formats simply delimit items by the newline character and can be used
 * for human readable output. The Avro RESTRepository uses this format to encode
 * subjects and schemas over HTTP. Subject names are forbidden from containing
 * whitespace. Schemas have their whitespace removed prior to use in the
 * Repository.
 * </p>
 */
public final class RepositoryUtil {
  private RepositoryUtil() {
  }

  /**
   * Encode {@link Subject}s into a {@link String} for use by
   * {@link #subjectNamesFromString(String)}
   * 
   * @param subjects
   *          the Subject objects to encode
   * @return The {@link Subject} objects encoded as a String
   */
  public static String subjectsToString(Iterable<Subject> subjects) {
    StringBuilder sb = new StringBuilder();
    for (Subject s : subjects) {
      sb.append(s.getName()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Decode a string created by {@link #subjectsToString(Iterable)}
   * 
   * @param str
   *          The String to decode
   * @return an {@link java.lang.Iterable} of {@link Subject}
   */
  public static Iterable<String> subjectNamesFromString(String str) {
    List<String> subjects = new ArrayList<String>();
    if (str != null && !str.isEmpty()) {
      String[] strs = str.split("\n");
      for (String s : strs) {
        subjects.add(s);
      }
    }
    return subjects;
  }

  /**
   * Encode {@link SchemaEntry} objects into a {@link String} for use by
   * {@link #schemasFromString(String)}
   * 
   * @param allEntries
   *          the SchemaEntry objects to encode
   * @return The {@link SchemaEntry} objects encoded as a String
   */
  public static String schemasToString(Iterable<SchemaEntry> allEntries) {
    StringBuilder sb = new StringBuilder();
    for (SchemaEntry s : allEntries) {
      sb.append(s.toString()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Decode a string created by {@link #schemasToString(Iterable)}
   * 
   * @param str
   *          The String to decode
   * @return An {@link java.lang.Iterable} of {@link SchemaEntry}
   */
  public static Iterable<SchemaEntry> schemasFromString(String str) {
    List<SchemaEntry> schemas = new ArrayList<SchemaEntry>();
    if (str != null && !str.isEmpty()) {
      String[] strs = str.split("\n");
      for (String s : strs) {
        schemas.add(new SchemaEntry(s));
      }
    }
    return schemas;
  }

  /**
   * Throws IllegalArgumentException if the string provided is null, or empty.
   */
  public static void validateSchemaOrSubject(String val) {
    if (null == val || val.isEmpty()) {
      throw new IllegalArgumentException(
          "Provided string is null or empty: '" + val + "'");
    }
  }
  
  /**
   * Returns an immutable Map<String, String> from the properties provided.
   * Includes any default values that exist in the properties. 
   */
  public static SubjectConfig configFromProperties(Properties props) {
    HashMap<String, String> propData = new HashMap<String, String>();
    for (String key :props.stringPropertyNames()) {
      propData.put(key, props.getProperty(key));
    }
    return new SubjectConfig.Builder().set(propData).build();
  }
  
  /** temporary until we have decided how to deal with null configs or create a SubjectConfig class **/
  public static SubjectConfig safeConfig(SubjectConfig config) {
    if (null == config) {
      return SubjectConfig.emptyConfig();
    } else {
      return config;
    }
  }
  
}
