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

package org.apache.avro.repo.server;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.avro.repo.Repository;
import org.apache.avro.repo.RepositoryUtil;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.SubjectConfig;

import com.sun.jersey.api.NotFoundException;

/**
 * {@link RESTRepository} Is a JSR-311 REST Interface to a {@link Repository}.
 * 
 * Combine with {@link RepositoryServer} to run an embedded REST server.
 */
@Singleton
@Produces(MediaType.TEXT_PLAIN)
@Path("/")
public class RESTRepository {

  private final Repository repo;

  /**
   * Create a {@link RESTRepository} that wraps a given {@link Repository}
   * Typically the wrapped repository is a
   * {@link org.apache.avro.repo.CacheRepository} that wraps a non-caching
   * underlying repository.
   * 
   * @param repo
   *          The {@link Repository} to wrap.
   */
  @Inject
  public RESTRepository(Repository repo) {
    this.repo = repo;
  }

  /**
   * @return All subjects in the repository, serialized with
   *         {@link RepositoryUtil#subjectsToString(Iterable)}
   */
  @GET
  public String allSubjects() {
    return RepositoryUtil.subjectsToString(repo.subjects());
  }

  /**
   * Returns all schemas in the given subject, serialized wity
   * {@link RepositoryUtil#schemasToString(Iterable)}
   * 
   * @param subject
   *          The name of the subject
   * @return all schemas in the subject. Return a 404 Not Found if there is no
   *         such subject
   */
  @GET
  @Path("{subject}/all")
  public String subjectList(@PathParam("subject") String subject) {
    Subject s = repo.lookup(subject);
    if (null == s) {
      throw new NotFoundException();
    }
    String result = RepositoryUtil.schemasToString(s.allEntries());
    return result;
  }
  
  @GET
  @Path("{subject}/config")
  public String subjectConfig(@PathParam("subject") String subject) {
    Subject s = repo.lookup(subject);
    if (null == s) {
      throw new NotFoundException();
    }
    Properties props = new Properties();
    props.putAll(s.getConfig().asMap());
    StringWriter writer = new StringWriter();
    try {
      props.store(writer, null);
    } catch (IOException e) {
      // stringWriter can't throw ... but just in case
      throw new RuntimeException(e);
    }
    return writer.toString();
  }

  /**
   * Create a subejct if it does not already exist.
   * 
   * @param subject
   *          the name of the subject
   * @param configParams
   *          the configuration values for the Subject, as form parameters
   * @return the subject name in a 200 response if successful. HTTP 404 if the
   *         subject does not exist, or HTTP 409 if there was a conflict
   *         creating the subject
   */
  @PUT
  @Path("{subject}")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  public Response createSubject(@PathParam("subject") String subject,
      MultivaluedMap<String, String> configParams) {
    if (null == subject) {
      return Response.status(400).build();
    }
    SubjectConfig.Builder builder = new SubjectConfig.Builder();
    for(Map.Entry<String, List<String>> entry : configParams.entrySet()) {
      List<String> val = entry.getValue();
      if(val.size() > 0) {
        builder.set(entry.getKey(), val.get(0));
      }
    }
    Subject created = repo.register(subject, builder.build());
    return Response.ok(created.getName()).build();
  }

  /**
   * Get the latest schema for a subject
   * 
   * @param subject
   *          the name of the subject
   * @return A 200 response with {@link SchemaEntry#toString()} as the body, or
   *         a 404 response if either the subject or latest schema is not found.
   */
  @GET
  @Path("{subject}/latest")
  public String latest(@PathParam("subject") String subject) {
    return exists(getSubject(subject).latest()).toString();
  }

  /**
   * Look up a schema by subject + id pair.
   * 
   * @param subject
   *          the name of the subject
   * @param id
   *          the id of the schema
   * @return A 200 response with the schema as the body, or a 404 response if
   *         the subject or schema is not found
   */
  @GET
  @Path("{subject}/id/{id}")
  public String schemaFromId(@PathParam("subject") String subject,
      @PathParam("id") String id) {
    return exists(getSubject(subject).lookupById(id)).getSchema();
  }

  /**
   * Look up an id by a subject + schema pair.
   * 
   * @param subject
   *          the name of the subject
   * @param schema
   *          the schema to search for
   * @return A 200 response with the id in the body, or a 404 response if the
   *         subject or schema is not found
   */
  @POST
  @Path("{subject}/schema")
  @Consumes(MediaType.TEXT_PLAIN)
  public String idFromSchema(@PathParam("subject") String subject, String schema) {
    return exists(getSubject(subject).lookupBySchema(schema)).getId();
  }

  /**
   * Register a schema with a subject
   * 
   * @param subject
   *          The subject name to register the schema in
   * @param schema
   *          The schema to register
   * @return A 200 response with the corresponding id if successful, a 403
   *         forbidden response if the schema fails validation, or a 404 not
   *         found response if the subject does not exist
   */
  @PUT
  @Path("{subject}/register")
  @Consumes(MediaType.TEXT_PLAIN)
  public Response addSchema(@PathParam("subject") String subject, String schema) {
    try {
      return Response.ok(getSubject(subject).register(schema).getId()).build();
    } catch (SchemaValidationException e) {
      return Response.status(Status.FORBIDDEN).build();
    }
  }

  /**
   * Register a schema with a subject, only if the latest schema equals the
   * expected value. This is for resolving race conditions between multiple
   * registrations and schema invalidation events in underlying repositories.
   * 
   * @param subject
   *          the name of the subject
   * @param latestId
   *          the latest schema id, possibly null
   * @param schema
   *          the schema to attempt to register
   * @return a 200 response with the id of the newly registered schema, or a 404
   *         response if the subject or id does not exist or a 409 conflict if
   *         the id does not match the latest id or a 403 forbidden response if
   *         the schema failed validation
   */
  @PUT
  @Path("{subject}/register_if_latest/{latestId: .*}")
  @Consumes(MediaType.TEXT_PLAIN)
  public Response addSchema(@PathParam("subject") String subject,
      @PathParam("latestId") String latestId, String schema) {
    Subject s = getSubject(subject);
    SchemaEntry latest;
    if ("".equals(latestId)) {
      latest = null;
    } else {
      latest = exists(s.lookupById(latestId));
    }
    SchemaEntry created = null;
    try {
      created = s.registerIfLatest(schema, latest);
      if (null == created) {
        return Response.status(Status.CONFLICT).build();
      }
      return Response.ok(created.getId()).build();
    } catch (SchemaValidationException e) {
      return Response.status(Status.FORBIDDEN).build();
    }
  }

  /**
   * Get a subject
   * 
   * @param subject
   *          the name of the subject
   * @return a 200 response if the subject exists, or a 404 response if the
   *         subject does not.
   */
  @GET
  @Path("{subject}")
  public Response checkSubject(@PathParam("subject") String subject) {
    getSubject(subject);
    return Response.ok().build();
  }
  
  @GET
  @Path("{subject}/integral")
  public String getSubjectIntegralKeys(@PathParam("subject") String subject) {
    return Boolean.toString(getSubject(subject).integralKeys());
  }

  private Subject getSubject(String subjectName) {
    Subject subject = repo.lookup(subjectName);
    if (null == subject) {
      throw new NotFoundException();
    }
    return subject;
  }

  private SchemaEntry exists(SchemaEntry entry) {
    if (null == entry) {
      throw new NotFoundException();
    }
    return entry;
  }

}
