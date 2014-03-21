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

package org.apache.avro.repo.client;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;

import org.apache.avro.repo.Repository;
import org.apache.avro.repo.RepositoryUtil;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.SubjectConfig;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

/**
 * An Implementation of {@link Repository} that connects to a remote
 * RESTRepository over HTTP.<br/>
 * <br/>
 * Typically, this is used in a client wrapped in a
 * {@link org.apache.avro.repo.CacheRepository} to limit network communication.<br/>
 * <br/>
 * Alternatively, this implementation can itself be what is used behind a
 * RESTRepository in a RepositoryServer, thus creating a caching proxy.
 */
public class RESTRepositoryClient implements Repository {

  private WebResource webResource;

  @Inject
  public RESTRepositoryClient(@Named("avro.repo.url") String url) {
    this.webResource = Client.create().resource(url);
  }

  @Override
  public Subject register(String subject, SubjectConfig config) {
    String path = subject;
    Form form = new Form();
    for(Map.Entry<String, String> entry : RepositoryUtil.safeConfig(config).asMap().entrySet()) {
      form.putSingle(entry.getKey(), entry.getValue());
    }

    String regSubjectName = webResource.path(path).accept(MediaType.TEXT_PLAIN)
        .type(MediaType.APPLICATION_FORM_URLENCODED).put(String.class, form);

    RESTSubject sub = new RESTSubject(regSubjectName);
    return sub;
  }

  @Override
  public Subject lookup(String subject) {
    RepositoryUtil.validateSchemaOrSubject(subject);
    try {//returns ok or exception if not found
      webResource.path(subject).get(String.class);
      return new RESTSubject(subject);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Iterable<Subject> subjects() {
    ArrayList<Subject> subjectList = new ArrayList<Subject>();
    try {
      String subjects = webResource.get(String.class);
      for (String subjName : RepositoryUtil.subjectNamesFromString(subjects)) {
        subjectList.add(new RESTSubject(subjName));
      }
    } catch (Exception e) {
      //no op. return empty list anyways
    }
    return subjectList;
  }

  private class RESTSubject extends Subject {

    private RESTSubject(String name) {
      super(name);
    }
    
    @Override
    public SubjectConfig getConfig() {
      String path = getName() + "/config" ;
      try {
        String propString = webResource.path(path).accept(MediaType.TEXT_PLAIN)
            .get(String.class);
        Properties props = new Properties();
        props.load(new StringReader(propString));
        return RepositoryUtil.configFromProperties(props);
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public SchemaEntry register(String schema) throws SchemaValidationException {
      RepositoryUtil.validateSchemaOrSubject(schema);

      String path = getName() + "/register";
      return handleRegisterRequest(path, schema);
    }

    @Override
    public SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
        throws SchemaValidationException {
      RepositoryUtil.validateSchemaOrSubject(schema);
      String idStr = (latest == null) ? "" : latest.getId();

      String path = getName() + "/register_if_latest/" + idStr;
      return handleRegisterRequest(path, schema);
    }

    private SchemaEntry handleRegisterRequest(String path, String schema)
        throws SchemaValidationException {
      String schemaId;
      try {
        schemaId = webResource.path(path).accept(MediaType.TEXT_PLAIN)
            .type(MediaType.TEXT_PLAIN_TYPE).put(String.class, schema);
        return new SchemaEntry(schemaId, schema);
      } catch (UniformInterfaceException e) {
        ClientResponse cr = e.getResponse();
        if (ClientResponse.Status.fromStatusCode(cr.getStatus()).equals(
            ClientResponse.Status.FORBIDDEN)) {
          throw new SchemaValidationException("Invalid schema: " + schema);
        } else {
          //any other status should return null
          return null;
        }
      } catch (ClientHandlerException e) {
        return null;
      }
    }

    @Override
    public SchemaEntry lookupBySchema(String schema) {
      RepositoryUtil.validateSchemaOrSubject(schema);
      String path = getName() + "/schema";
      try {
        String schemaId = webResource.path(path).accept(MediaType.TEXT_PLAIN)
            .type(MediaType.TEXT_PLAIN_TYPE).post(String.class, schema);
        return new SchemaEntry(schemaId, schema);
      } catch (UniformInterfaceException e) {
        return null;
      }
    }

    @Override
    public SchemaEntry lookupById(String schemaId) {
      RepositoryUtil.validateSchemaOrSubject(schemaId);
      String path = getName() + "/id/" + schemaId;
      try {
        String schema = webResource.path(path).get(String.class);
        return new SchemaEntry(schemaId, schema);
      } catch (UniformInterfaceException e) {
        return null;
      }
    }

    @Override
    public SchemaEntry latest() {
      String path = getName() + "/latest";
      String entryStr;
      try {
        entryStr = webResource.path(path).get(String.class);
        return new SchemaEntry(entryStr);
      } catch (UniformInterfaceException e) {
        return null;
      }
    }

    @Override
    public Iterable<SchemaEntry> allEntries() {
      String path = getName() + "/all";
      try {
        String entriesStr = webResource.path(path).get(String.class);
        return schemaEntriesFromStr(entriesStr);
      } catch (UniformInterfaceException e) {
        return Collections.emptyList();
      } 
    }

    private Iterable<SchemaEntry> schemaEntriesFromStr(String entriesStr) {
      return RepositoryUtil.schemasFromString(entriesStr);
    }
    
    @Override
    public boolean integralKeys() {
      try {
        String path = getName() + "/integral";
        String integral = webResource.path(path).get(String.class);
        return Boolean.parseBoolean(integral);
      } catch (UniformInterfaceException e){
        return false;
      }
    }

  }

}
