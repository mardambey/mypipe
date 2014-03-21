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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * A {@link RepositoryServer} is a stand-alone server for running a
 * {@link RESTRepository}. {@link #main(String...)} takes a single argument
 * containing a property file for configuration. <br/>
 * <br/>
 * 
 */
public class RepositoryServer {
  private final Server server;

  /**
   * Constructs an instance of this class, overlaying the default properties
   * with any identically-named properties in the supplied {@link Properties}
   * instance.
   * 
   * @param props
   *          Property values for overriding the defaults.
   *          <p>
   *          <b><i>Any overriding properties must be supplied as type </i>
   *          <code>String</code><i> or they will not work and the default
   *          values will be used.</i></b>
   * 
   */
  public RepositoryServer(Properties props) {
    Injector injector = Guice.createInjector(
        new ConfigModule(props), 
        new ServerModule());
    this.server = injector.getInstance(Server.class);
  }

  public static void main(String... args) throws Exception {
    if (args.length != 1) {
      printHelp();
      System.exit(1);
    }
    File config = new File(args[0]);
    if (!config.canRead()) {
      System.err.println("Cannot read file: " + config);
      printHelp();
      System.exit(1);
    }
    Properties props = new Properties();
    props.load(new BufferedInputStream(new FileInputStream(config)));

    RepositoryServer server = new RepositoryServer(props);
    try {
      server.start();
      server.join();
    } finally {
      server.stop();
    }
  }

  public void start() throws Exception {
    server.start();
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public void stop() throws Exception {
    server.stop();
  }

  private static void printHelp() {
    System.err.println("One argument expected containing a configuration "
        + "properties file.  Default properties are:");
    ConfigModule.printDefaults(System.err);
  }

  private static class ServerModule extends JerseyServletModule {

    @Override
    protected void configureServlets() {
      bind(Connector.class).to(SelectChannelConnector.class);
      serve("/*").with(GuiceContainer.class);
      bind(RESTRepository.class);
    }

    @Provides
    @Singleton
    public Server provideServer(
        @Named(ConfigModule.JETTY_HOST) String host,
        @Named(ConfigModule.JETTY_PORT) Integer port,
        @Named(ConfigModule.JETTY_PATH) String path,
        @Named(ConfigModule.JETTY_HEADER_SIZE) Integer headerSize,
        @Named(ConfigModule.JETTY_BUFFER_SIZE) Integer bufferSize,
        Connector connector,
        GuiceFilter guiceFilter,
        ServletContextHandler handler) {

      Server server = new Server();
      if (null != host && !host.isEmpty()) {
        connector.setHost(host);
      }
      connector.setPort(port);
      connector.setRequestHeaderSize(headerSize);
      connector.setRequestBufferSize(bufferSize);
      server.setConnectors(new Connector[] { connector });

      // the guice filter intercepts all inbound requests and uses its bindings
      // for servlets 
      FilterHolder holder = new FilterHolder(guiceFilter);
      handler.addFilter(holder, "/*", null);
      handler.addServlet(NoneServlet.class, "/");
      handler.setContextPath(path);
      server.setHandler(handler);
      server.dumpStdErr();
      return server;
    }

    private static final class NoneServlet extends HttpServlet {
      private static final long serialVersionUID = 4560115319373180139L;
    }
  }

}
