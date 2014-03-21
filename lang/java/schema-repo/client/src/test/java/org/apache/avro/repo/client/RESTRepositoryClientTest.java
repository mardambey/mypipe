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

import java.util.Properties;

import org.apache.avro.repo.AbstractTestRepository;
import org.apache.avro.repo.InMemoryRepository;
import org.apache.avro.repo.server.RepositoryServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class RESTRepositoryClientTest extends
    AbstractTestRepository<RESTRepositoryClient> {

  static RepositoryServer server;
  static RESTRepositoryClient client;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties props = new Properties();
    props.put("repo.class", InMemoryRepository.class.getName());
    props.put("jetty.host", "localhost");
    props.put("jetty.port", "8123");
    server = new RepositoryServer(props);
    server.start();
  }

  @Override
  protected RESTRepositoryClient createRepository() {
    return new RESTRepositoryClient("http://localhost:8123/schema-repo/");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    server.stop();
  }

}
