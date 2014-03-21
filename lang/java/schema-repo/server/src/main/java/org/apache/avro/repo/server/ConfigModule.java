package org.apache.avro.repo.server;

import java.io.PrintStream;
import java.util.Properties;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.avro.repo.CacheRepository;
import org.apache.avro.repo.InMemoryCache;
import org.apache.avro.repo.Repository;
import org.apache.avro.repo.RepositoryCache;
import org.apache.avro.repo.Validator;
import org.apache.avro.repo.ValidatorFactory;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * A {@link Module} for configuration based on a set of {@link Properties} 
 * <br/>
 * Binds every property value in the properties provided to the property name
 * in Guice, making them available with the {@link Named} annotation.  Guice
 * will automatically convert these to constant values, such as Integers,
 * Strings, or Class constants.
 * <br/>
 * Keys starting with "validator." bind {@link Validator} classes 
 * in a {@link ValidatorFactory}, where the name is the remainder of the key
 * following "validator.".  For example, a property 
 * "validtator.backwards_compatible=com.foo.BackwardsCompatible"
 * will set a validator named "backwards_compatible" to an instance of the
 * class com.foo.BackwardsCompatible.
 */
class ConfigModule implements Module {
  static final String JETTY_HOST = "jetty.host";
  static final String JETTY_PORT = "jetty.port";
  static final String JETTY_PATH = "jetty.path";
  static final String JETTY_HEADER_SIZE = "jetty.header.size";
  static final String JETTY_BUFFER_SIZE = "jetty.buffer.size";
  private static final String REPO_CLASS = "repo.class";
  private static final String REPO_CACHE = "repo.cache";

  private static final Properties DEFAULTS = new Properties();
  static {
    DEFAULTS.setProperty(JETTY_HOST, "");
    DEFAULTS.setProperty(JETTY_PORT, "2876"); // 'AVRO' on a t-9 keypad
    DEFAULTS.setProperty(JETTY_PATH, "/schema-repo");
    DEFAULTS.setProperty(JETTY_HEADER_SIZE, "16384");
    DEFAULTS.setProperty(JETTY_BUFFER_SIZE, "16384");
    DEFAULTS.setProperty(REPO_CACHE, InMemoryCache.class.getName());
  }
  
  public static void printDefaults(PrintStream writer) {
    writer.println(DEFAULTS);
  }
  
  private final Properties props;

  public ConfigModule(Properties props) {
    Properties copy = new Properties(DEFAULTS);
    copy.putAll(props);
    this.props = copy;
  }
  
  @Override
  public void configure(Binder binder) {
    Names.bindProperties(binder, props);
  }
  
  @Provides
  @Singleton
  Repository provideRepository(Injector injector,
      @Named(REPO_CLASS) Class<Repository> repoClass,
      @Named(REPO_CACHE) Class<RepositoryCache> cacheClass) {
    Repository repo = injector.getInstance(repoClass);
    RepositoryCache cache = injector.getInstance(cacheClass);
    return new CacheRepository(repo, cache);
  }
  
  @Provides
  @Singleton
  ValidatorFactory provideValidatorFactory(Injector injector) {
    ValidatorFactory.Builder builder = new ValidatorFactory.Builder();
    for(String prop : props.stringPropertyNames()) {
      if (prop.startsWith("validator.")) {
        String validatorName = prop.substring("validator.".length());
        Class<Validator> validatorClass = injector.getInstance(
            Key.<Class<Validator>>get(
                new TypeLiteral<Class<Validator>>(){}, Names.named(prop)));
        builder.setValidator(validatorName, injector.getInstance(validatorClass));
      }
    }
    return builder.build();
  }
}

