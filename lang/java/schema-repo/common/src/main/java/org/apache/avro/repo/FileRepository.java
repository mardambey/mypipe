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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * A {@link Repository} that persists content to file. <br/>
 * <br/>
 * The {@link Repository} stores all of its data in a single base directory.
 * Within this directory each {@link Subject} is represented by a nested
 * directory with the same name as the {@link Subject}. Within each
 * {@link Subject} directory there are three file types: <li>
 * A properties file named 'subject.properties' containing the configured
 * properties for the Subject. At this time, the only used property is
 * "avro.repo.validator.class".</li> <li>
 * A text file named 'schema_ids' containing the schema ids, in order of their
 * creation, delimited by newline, encoded in UTF-8. This is used to track the
 * order of schema registration for {@link Subject#latest()} and
 * {@link Subject#allEntries()}</li> <li>
 * One file per schema the contents of which are the schema encoded in UTF-8 and
 * the name of which is the schema id followed by the postfix '.schema'.</li>
 * 
 */
public class FileRepository implements Repository, Closeable {

  private static final String LOCKFILE = ".repo.lock";
  private static final String SUBJECT_PROPERTIES = "subject.properties";
  private static final String SCHEMA_IDS = "schema_ids";
  private static final String SCHEMA_POSTFIX = ".schema";

  private final InMemorySubjectCache subjects = new InMemorySubjectCache();
  private final ValidatorFactory validators;
  private final File rootDir;
  private final FileChannel lockChannel;
  private final FileLock fileLock;
  private boolean closed = false;

  /**
   * Create a FileRepository in the directory path provided. Locks a file
   * "repository.lock" to ensure no other object or process is running a
   * FileRepository from the same place. The lock is released if
   * {@link #close()} is called, the object is finalized, or the JVM exits.
   * 
   * Not all platforms support file locks. See {@link FileLock}
   * 
   * @param repoPath
   *          The
   */
  @Inject
  public FileRepository(@Named("avro.repo.file-repo-path") String repoPath, ValidatorFactory validators) {
    this.validators = validators;
    this.rootDir = new File(repoPath);
    if ((!rootDir.exists() && !rootDir.mkdirs()) || !rootDir.isDirectory()) {
      throw new java.lang.RuntimeException(
          "Unable to create repo directory, or not a directory: "
              + rootDir.getAbsolutePath());
    }
    // lock repository
    try {
      File lockfile = new File(rootDir, LOCKFILE);
      lockfile.createNewFile();
      @SuppressWarnings("resource") // raf is closed when lockChannel is closed
      RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
      lockChannel = raf.getChannel();
      fileLock = lockChannel.tryLock();
      if (fileLock != null) {
        lockfile.deleteOnExit();
      } else {
        throw new IllegalStateException("Failed to lock file: "
            + lockfile.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to lock repository directory: "
          + rootDir.getAbsolutePath(), e);
    }
    // eagerly load up subjects
    loadSubjects(rootDir, subjects);
  }

  private void loadSubjects(File repoDir, SubjectCache subjects) {
    for (File file : repoDir.listFiles()) {
      if (file.isDirectory()) {
        subjects.add(new FileSubject(file));
      }
    }
  }

  private void isValid() {
    if (closed) {
      throw new IllegalStateException("FileRepository is closed");
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    try {
      fileLock.release();
    } catch (IOException e) {
      // nothing to do here -- it was already released
      // or there are underlying errors we cannot recover from
    } finally {
      closed = true;
      try {
        lockChannel.close();
      } catch (IOException e) {
        // nothing to do here -- underlying errors but recovery
        // not possible here or in client, and already closed
      }
    }
  }

  @Override
  public synchronized Subject register(String subjectName, SubjectConfig config) {
    isValid();
    Subject subject = subjects.lookup(subjectName);
    if (null == subject) {
      subject = subjects.add(Subject.validatingSubject(createNewFileSubject(subjectName, config), validators));
    }
    return subject;
  }

  @Override
  public synchronized Subject lookup(String subjectName) {
    isValid();
    return subjects.lookup(subjectName);
  }

  @Override
  public synchronized Iterable<Subject> subjects() {
    isValid();
    return subjects.values();
  }

  private FileSubject createNewFileSubject(String subject,
      SubjectConfig config) {
    File subjectDir = new File(rootDir, subject);
    createNewSubjectDir(subjectDir, config);
    return new FileSubject(subjectDir);
  }

  // create a new empty subject directory 
  private static void createNewSubjectDir(File subjectDir, SubjectConfig config) {
    if (subjectDir.exists()) {
      throw new RuntimeException(
          "Cannot create a FileSubject, directory already exists: "
              + subjectDir.getAbsolutePath());
    }
    if (!subjectDir.mkdir()) {
      throw new RuntimeException("Cannot create a FileSubject dir: "
          + subjectDir.getAbsolutePath());
    }

    createNewFileInDir(subjectDir, SCHEMA_IDS);
    File subjectProperties = createNewFileInDir(subjectDir, SUBJECT_PROPERTIES);

    Properties props = new Properties();
    props.putAll(RepositoryUtil.safeConfig(config).asMap());
    writePropertyFile(subjectProperties, props);

  }

  private static File createNewFileInDir(File dir, String filename) {
    File result = new File(dir, filename);
    try {
      if (!result.createNewFile()) {
        throw new RuntimeException(result.getAbsolutePath() + " already exists");
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to create file: "
          + result.getAbsolutePath(), e);
    }
    return result;
  }

  private static void writeToFile(File file, WriteOp op, boolean append) {
    FileOutputStream out;
    try {
      out = new FileOutputStream(file, append);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Could not open file for write: "
          + file.getAbsolutePath());
    }
    try {
      OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
      BufferedWriter bwriter = new BufferedWriter(writer);
      op.write(bwriter);
      bwriter.flush();
      bwriter.close();
      writer.close();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write and close file "
          + file.getAbsolutePath());
    }
  }

  private static void writePropertyFile(File file, final Properties prop) {
    writeToFile(file, new WriteOp() {
      @Override
      protected void write(Writer writer) throws IOException {
        prop.store(writer, "Schema Repository Subject Properties");
      }
    }, false);
  }

  private static void appendLineToFile(File file, final String line) {
    writeToFile(file, new WriteOp() {
      @Override
      protected void write(Writer writer) throws IOException {
        writer.append(line).append('\n');
      }
    }, true);
  }

  private static void dirExists(File dir) {
    if (!dir.exists() || !dir.isDirectory()) {
      throw new RuntimeException(
          "directory does not exist or is not a directory: " + dir.toString());
    }
  }

  private static void fileReadable(File file) {
    if (!file.canRead()) {
      throw new RuntimeException("file does not exist or is not readable: "
          + file.toString());
    }
  }

  private static void fileWriteable(File file) {
    if (!file.canWrite()) {
      throw new RuntimeException("file does not exist or is not writeable: "
          + file.toString());
    }
  }

  private abstract static class WriteOp {
    protected abstract void write(Writer writer) throws IOException;
  }

  private class FileSubject extends Subject {
    private final File subjectDir;
    private final File idFile;
    private final File propertyFile;
    private final SubjectConfig config;
    
    private int largestId = -1;
    private SchemaEntry latest;

    private FileSubject(File dir) {
      super(dir.getName());
      this.subjectDir = dir;
      this.idFile = new File(dir, SCHEMA_IDS);
      this.propertyFile = new File(dir, SUBJECT_PROPERTIES);
      dirExists(subjectDir);
      fileReadable(idFile);
      fileWriteable(idFile);
      fileReadable(propertyFile);
      fileWriteable(propertyFile);

      // read from config file
      Properties props = new Properties();
      try {
        props.load(new FileInputStream(propertyFile));
        config = RepositoryUtil.configFromProperties(props);
        Integer lastId = null;
        HashSet<String> schemaFileNames = getSchemaFiles();
        HashSet<Integer> foundIds = new HashSet<Integer>();
        for (Integer id : getSchemaIds()) {
          if (id > largestId) {
            largestId = id;
          }
          lastId = id;
          if(!foundIds.add(id)) {
            throw new RuntimeException("Corrupt id file, id '" + id + 
                "' duplicated in " + idFile.getAbsolutePath());
          }
          fileReadable(getSchemaFile(id));
          schemaFileNames.remove(getSchemaFileName(id));
        }
        if (schemaFileNames.size() > 0) {
          throw new RuntimeException("Schema files found in subject directory "
              + subjectDir.getAbsolutePath()
              + " that are not referenced in the " + SCHEMA_IDS + " file: "
              + schemaFileNames.toString());
        }
        if (lastId != null) {
          latest = new SchemaEntry(lastId.toString(),
              readSchemaForId(lastId.toString()));
        }
      } catch (IOException e) {
        throw new RuntimeException("error initializing subject: "
            + subjectDir.getAbsolutePath(), e);
      }
    }
    
    @Override
    public SubjectConfig getConfig() {
      return config;
    }

    @Override
    public synchronized SchemaEntry register(String schema)
        throws SchemaValidationException {
      isValid();
      RepositoryUtil.validateSchemaOrSubject(schema);
      SchemaEntry entry = lookupBySchema(schema);
      if (entry == null) {
        entry = createNewSchemaFile(schema);
        appendLineToFile(idFile, entry.getId());
        latest = entry;
      }
      return entry;
    }

    private synchronized SchemaEntry createNewSchemaFile(String schema) {
      try {

        int newId = largestId + 1;
        File f = getSchemaFile(String.valueOf(newId));
        if (!f.exists() && f.createNewFile()) {
          Writer output = new BufferedWriter(new FileWriter(f));
          try {
            output.write(schema);
            output.flush();
          } finally {
            output.close();
          }

          latest = new SchemaEntry(String.valueOf(newId), schema);
          largestId++;
          return latest;
        } else {
          throw new RuntimeException(
              "Unable to register schema, schema file either exists already "
                  + " or couldn't create new file");
        }
      } catch (NumberFormatException e) {
        throw new RuntimeException(
            "Unable to register schema, invalid schema latest schema id ", e);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to register schema, couldn't create schema file ", e);
      }

    }

    @Override
    public synchronized SchemaEntry registerIfLatest(String schema,
        SchemaEntry latest) throws SchemaValidationException {
      isValid();
      if (latest == this.latest // both null
          || (latest != null && latest.equals(this.latest))) {
        return register(schema);
      } else {
        return null;
      }
    }

    @Override
    public synchronized SchemaEntry lookupBySchema(String schema) {
      isValid();
      RepositoryUtil.validateSchemaOrSubject(schema);
      for (Integer id : getSchemaIds()) {
        String idStr = id.toString();
        String schemaInFile = readSchemaForIdOrNull(idStr);
        if (schema.equals(schemaInFile)) {
          return new SchemaEntry(idStr, schema);
        }
      }
      return null;
    }

    @Override
    public synchronized SchemaEntry lookupById(String id) {
      isValid();
      String schema = readSchemaForIdOrNull(id);
      if (schema != null) {
        return new SchemaEntry(id, schema);
      } 
      return null;
    }

    @Override
    public synchronized SchemaEntry latest() {
      isValid();
      return latest;
    }

    @Override
    public synchronized Iterable<SchemaEntry> allEntries() {
      isValid();
      List<SchemaEntry> entries = new ArrayList<SchemaEntry>();
      for (Integer id : getSchemaIds()) {
          String idStr = id.toString();
          String schema = readSchemaForId(idStr);
          entries.add(new SchemaEntry(idStr, schema));
      }
      Collections.reverse(entries);
      return entries;
    }
    
    @Override
    public boolean integralKeys() {
      return true;
    }
    
    private String readSchemaForIdOrNull(String id) {
      try {
        return readSchemaForId(id);
      } catch (Exception e) {
        return null;
      }
    }
    
    private String readSchemaForId(String id) {
      File schemaFile = getSchemaFile(id);
      return readSchemaFile(schemaFile);
    }
    
    private String readSchemaFile(File schemaFile) {
      try {
        return readAllAsString(schemaFile);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(
            "Could not read schema contents at: "
                + schemaFile.getAbsolutePath(), e);
      }
    }

    private String readAllAsString(File file) throws FileNotFoundException {
      // a scanner that will read a whole file
      Scanner s = new Scanner(file, "UTF-8").useDelimiter("\\A");
      try {
        return s.nextLine();
      } catch (NoSuchElementException e) {
        throw new RuntimeException(
            "file is empty: " + file.getAbsolutePath(), e);
      } finally {
        s.close();
      }
    }
    
    private HashSet<String> getSchemaFiles() {
      String[] files = subjectDir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (null != name && name.endsWith(SCHEMA_POSTFIX)) {
            return true;
          }
          return false;
         }
      });
      return new HashSet<String>(Arrays.asList(files));
    }

    // schema ids from the schema id file, in order from oldest to newest
    private List<Integer> getSchemaIds(){
      Scanner s = getIdFileScanner();
      List<Integer> ids = new ArrayList<Integer>();
      try {
        while (s.hasNextLine()) {
          if(s.hasNext()) {
            // only read non-empty lines
            ids.add(s.nextInt());
          }
          s.nextLine();
        }
        return ids;
      } finally {
        s.close();
      }
    }
    
    private Scanner getIdFileScanner() {
      try {
        return new Scanner(idFile, "UTF-8");
      } catch (FileNotFoundException e) {
        throw new RuntimeException("Unable to read schema id file: "
            + idFile.getAbsolutePath(), e);
      }
    }

    private File getSchemaFile(String id) {
      return new File(subjectDir, getSchemaFileName(id));
    }
    
    private File getSchemaFile(int id) {
      return getSchemaFile(String.valueOf(id));
    }
    
    private String getSchemaFileName(String id) {
      return id + SCHEMA_POSTFIX;
    }
    
    private String getSchemaFileName(int id) {
      return getSchemaFileName(String.valueOf(id));
    }

  }

}
