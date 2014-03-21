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
import java.util.List;

/**
 * A {@link Subject} is a collection of mutually compatible Schemas. <br/>
 * <br/>
 * Validation of schemas is pluggable and each subject may have its own
 * validation rules defined with its own {@link Validator} when registered with
 * a {@link Repository}. To create a {@link Subject} that validates its schemas,
 * use {@link #validateWith(Subject, Validator)}. <br/>
 * <br/>
 * Caching of schemas is pluggable via
 * {@link #cacheWith(Subject, SchemaEntryCache)}. A {@link Subject} can only
 * cache the schema to id mappings, as other properties of a Subject are not
 * safe to cache. <br/>
 * <br/>
 * A {@link Subject} has a few basic methods for interacting with Schemas: 
 * <li>
 * {@link #register(String)} attempts to register a schema with the Subject,
 * according to the validation rules of the Subject. The operation is idempotent
 * -- the return value is the {@link SchemaEntry} corresponding to the schema
 * String whether the schema existed before the operation or not.</li>
 * <li>
 * {@link #registerIfLatest(String, SchemaEntry)} attempts to register a schema
 * with the Subject, according to the validation rules of the Subject. The
 * operation succeeds only if the provided {@code latest} value is the current
 * latest schema in the system, and returns null otherwise.</li>
 * <li>
 * {@link #lookupById(String)} looks up a schema by its id, and returns null if
 * such a schema does not exist. Since the mapping from id to schema is
 * immutable, this result is cacheable.</li>
 * <li>
 * {@link #lookupBySchema(String)} looks up an id for a schema, and returns null
 * if no such schema exists. Since the mapping from id to schema is immutable,
 * this result is cacheable.</li>
 * <li>
 * {@link #allEntries()} returns all the schema entries for the subject, ordered
 * from most recent to oldest. The result is not cacheable, since additional
 * entries may be added.</li>
 * 
 */
public abstract class Subject {
  private final String name;

  /**
   * A {@link Subject} has a name. The name must not be null or empty, and
   * cannot contain whitespace. If the name contains whitespace an
   * {@link IllegalArgumentException} is thrown.
   **/
  protected Subject(String name) {
    RepositoryUtil.validateSchemaOrSubject(name);
    this.name = name;
  }

  /**
   * Return the Name of the Subject. A Subject name can not contain whitespace,
   * and must not be empty or null.
   */
  public String getName() {
    return name;
  }
  
  public abstract SubjectConfig getConfig();
  
  public abstract boolean integralKeys();

  /**
   * If the provided schema has already been registered in this subject, return
   * the id.
   * 
   * If the provided schema has not been registered in this subject, register it
   * and return its id.
   * 
   * Idempotent -- If two users simultaneously register the same schema, they
   * will both get the same {@link SchemaEntry} result and succeed.
   * 
   * @param schema
   *          The schema to register
   * @return The id of the schema
   * @throws SchemaValidationException
   *           If the schema change is not valid according the validation rules
   *           of the subject
   */
  public abstract SchemaEntry register(String schema)
      throws SchemaValidationException;

  /**
   * Register the provided schema only if the current latest schema matches the
   * provided latest entry.
   * 
   * 
   * @param schema
   *          The schema to register
   * @param latest
   *          the entry that must match the current actual latest value in order
   *          to register this schema.
   * @return The id of the schema, or null if latest does not match.
   * @throws SchemaValidationException
   *           If the schema change is not valid according the validation rules
   *           of the subject
   */
  public abstract SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
      throws SchemaValidationException;

  /**
   * Lookup the {@link SchemaEntry} for the given schema. Since the mapping of
   * schema to id is immutable, this result can be cached.
   * 
   * @param schema
   *          The schema to look up
   * @return The SchemaEntry of the schema or null if the schema is not
   *         registered
   */
  public abstract SchemaEntry lookupBySchema(String schema);

  /**
   * Lookup the {@link SchemaEntry} for the given subject by id. Since the
   * mapping of schema to id is immutable the result can be cached.
   * 
   * @param id
   *          the id of the schema to look up
   * @return The SchemaEntry of the schema or null if no such schema is
   *         registered for the provided id
   */
  public abstract SchemaEntry lookupById(String id);

  /**
   * Lookup the most recently registered schema for the given subject. This
   * result is not cacheable, since the latest schema may change.
   * 
   * @return The {@link SchemaEntry} or null if no schema is registered with
   *         this subject
   */
  public abstract SchemaEntry latest();

  /**
   * List the ids of schemas registered with the given subject, ordered from
   * most recent to oldest. This result is not cacheable, since the
   * {@link SchemaEntry} in the subject may grow over time.
   * 
   * @return the {@link SchemaEntry} objects in this subject, ordered from most
   *         recent to oldest.
   */
  public abstract Iterable<SchemaEntry> allEntries();

  @Override
  public String toString() {
    return name;
  }
  
  /**
   * Create a {@link Subject} that rejects modifications, throwing
   * {@link IllegalStateException} if a modification is attempted
   **/
  public static final Subject readOnly(Subject subject) {
    if (null == subject) {
      return subject;
    } else {
      return new ReadOnlySubject(subject);
    }
  }
  
  private static class ReadOnlySubject extends DelegatingSubject {
    private ReadOnlySubject(Subject subject) {
      super(subject);
    }
  
    @Override
    public SchemaEntry register(String schema) throws SchemaValidationException {
      throw new IllegalStateException("Cannot register, subject is read-only");
    }
  
    @Override
    public SchemaEntry registerIfLatest(String schema, SchemaEntry latest) {
      throw new IllegalStateException("Cannot register, subject is read-only");
    }

  }
  
  /**
   * Create a {@link Subject} that validates schemas as configured.
   */
  public static Subject validatingSubject(Subject subject, ValidatorFactory factory) {
    if (null == subject) {
      return subject;
    } 
    List<Validator> validators = factory.getValidators(subject.getConfig().getValidators());
    if (!validators.isEmpty()) {
      return new ValidatingSubject(subject, new CompositeValidator(validators));
    } else {
      return subject;
    }
  }

  private static final class CompositeValidator implements Validator {
    private final ArrayList<Validator> validators;
    private CompositeValidator(List<Validator> validators) {
      this.validators = new ArrayList<Validator>(validators);
    }

    @Override
    public void validate(String schemaToValidate,
        Iterable<SchemaEntry> schemasInOrder) throws SchemaValidationException {
      for(Validator v : validators) {
        v.validate(schemaToValidate, schemasInOrder);
      }
    }
  }

  private static class ValidatingSubject extends DelegatingSubject {
    protected final Validator validator;

    private ValidatingSubject(Subject delegate, Validator validator) {
      super(delegate);
      this.validator = validator;
    }

    @Override
    public SchemaEntry register(String schema) throws SchemaValidationException {
      while (true) {
        Iterable<SchemaEntry> schemaEntries = allEntries();
        SchemaEntry actualLatest = null;
        for (SchemaEntry entry : schemaEntries) {
          actualLatest = entry;
          break;
        }
        validator.validate(schema, schemaEntries);
        SchemaEntry registered = super.registerIfLatest(schema, actualLatest);
        // if registered is not null, it was successful
        if (null != registered) {
          return registered;
        }
      }
    }

    @Override
    public SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
        throws SchemaValidationException {
      Iterable<SchemaEntry> schemaEntries = allEntries();
      SchemaEntry actualLatest = null;
      for (SchemaEntry entry : schemaEntries) {
        actualLatest = entry;
        break;
      }
      if (actualLatest == latest
          || ((actualLatest != null) && actualLatest.equals(latest))) {
        // they are equal, either both are null or they equal
        validator.validate(schema, schemaEntries);
        return super.registerIfLatest(schema, latest);
      } else {
        return null;
      }
    }
  }

  /**
   * Create a {@link Subject} that caches id to schema mappings using the
   * {@link SchemaEntryCache} provided.
   * 
   * @param subject
   *          The {@link Subject} to wrap
   * @param cache
   *          The {@link SchemaEntryCache} to cache with
   * @return returns a {@link Subject} instance that caches {@link SchemaEntry}
   *         values with the cache provided, if and only if both parameters are
   *         not null. <br/>
   *         If the provided subject is null, returns null. If the provided
   *         cache is null, returns the provided subject without wrapping it.
   */
  public static Subject cacheWith(Subject subject, SchemaEntryCache cache) {
    return (null == subject || null == cache) ? 
        subject : new CachingSubject(subject, cache);
  }

  private static class CachingSubject extends DelegatingSubject {
    private final SchemaEntryCache cache;

    private CachingSubject(Subject delegate, SchemaEntryCache cache) {
      super(delegate);
      this.cache = cache;
    }

    @Override
    public SchemaEntry register(String schema) throws SchemaValidationException {
      SchemaEntry entry = cache.lookupBySchema(schema);
      if (entry == null) {
        return cache.add(super.register(schema));
      }
      return entry;
    }

    @Override
    public SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
        throws SchemaValidationException {
      return cache.add(super.registerIfLatest(schema, latest));
    }

    @Override
    public SchemaEntry lookupBySchema(String schema) {
      SchemaEntry entry = cache.lookupBySchema(schema);
      if (entry == null) {
        return cache.add(super.lookupBySchema(schema));
      }
      return entry;
    }

    @Override
    public SchemaEntry lookupById(String id) {
      SchemaEntry entry = cache.lookupById(id);
      if (entry == null) {
        return cache.add(super.lookupById(id));
      }
      return entry;
    }

    @Override
    public Iterable<SchemaEntry> allEntries() {
      Iterable<SchemaEntry> all = super.allEntries();
      for (SchemaEntry entry : all) {
        cache.add(entry);
      }
      return all;
    }
  }

}
