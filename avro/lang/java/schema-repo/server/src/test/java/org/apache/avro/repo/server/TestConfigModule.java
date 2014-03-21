package org.apache.avro.repo.server;

import java.util.Properties;

import org.apache.avro.repo.InMemoryRepository;
import org.apache.avro.repo.Repository;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.SubjectConfig;
import org.apache.avro.repo.Validator;
import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestConfigModule {

  @Test
  public void testConfig() {
    Properties props = new Properties();
    props.setProperty("repo.class", InMemoryRepository.class.getName());
    props.put("validator.rejectAll", Reject.class.getName());
    ConfigModule module = new ConfigModule(props);
    Injector injector = Guice.createInjector(module);
    Repository repo = injector.getInstance(Repository.class);
    Subject rejects = repo.register("rejects", new SubjectConfig.Builder()
        .addValidator("rejectAll").build());
    boolean threw = false;
    try {
      rejects.register("stuff");
    } catch (SchemaValidationException se) {
      threw = true;
    }
    Assert.assertTrue(threw);
  }

  @Test
  public void testPrintDefaults() {
    ConfigModule.printDefaults(System.out);
  }

  public static class Reject implements Validator {
    @Override
    public void validate(String schemaToValidate,
        Iterable<SchemaEntry> schemasInOrder) throws SchemaValidationException {
      throw new SchemaValidationException("no");
    }
  }

}
