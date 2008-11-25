package org.hivedb.util.database.test;

import org.hivedb.*;
import org.hivedb.configuration.entity.EntityHiveConfig;
import org.hivedb.hibernate.BaseDataAccessObjectFactory;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.DataAccessObject;
import org.hivedb.persistence.CachingDataSourceProvider;
import org.hivedb.util.HiveDestructor;
import org.hivedb.util.YamlHiveCreator;
import org.hivedb.util.database.HiveDbDialect;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class HiveTest {
  protected EntityHiveConfig config;
  protected Hive hive;
  protected ConfigurationReader configurationReader;
  protected HiveDbDialect dialect;

  protected String getHiveConfigurationFile() {
    return getClass().isAnnotationPresent(Config.class) ?
      String.format("src/test/resources/%s.cfg.yml", getClass().getAnnotation(Config.class).value()) :
      String.format("src/test/resources/hive_%s.cfg.yml", getClass().getSimpleName());
  }

  protected void setup() {
    // override if needed;
  }

  protected void teardown() {
    // override if needed;
  }

  protected HiveDbDialect getDialect() {
    // override if needed
    return HiveDbDialect.H2;
  }

  @Before
  public void beforeMethod() throws Exception {
    dialect = getDialect();
    hive = new YamlHiveCreator(dialect).load(getHiveConfigurationFile());
    configurationReader = new ConfigurationReader(getMappedClasses());
    configurationReader.install(hive);
    config = getEntityHiveConfig();
    setup();
  }

  @After
  public void afterMethod() {
    teardown();
    new HiveDestructor().destroy(hive);
  }

  @SuppressWarnings("unchecked")
  public DataAccessObject<? extends Object, ? extends Serializable> getDao(Class clazz) {
    return new BaseDataAccessObjectFactory<Object, Serializable>(
      getEntityHiveConfig(),
      getMappedClasses(),
      clazz,
      getHive()).create();
  }

  public Hive getHive() {
    return hive;
  }

  public EntityHiveConfig getEntityHiveConfig() {
    return configurationReader.getHiveConfiguration();
  }

  @SuppressWarnings("unchecked")
  protected Collection<Class<?>> getMappedClasses() {
    return Arrays.asList(
      getPartitionDimensionClass(),
      WeatherReport.class,
      WeatherEvent.class);
  }

  protected Class<?> getPartitionDimensionClass() {
    return Continent.class;
  }

  protected String getHiveDatabaseName() {
    //return "hive";
    return H2TestCase.TEST_DB;
  }

  protected String getConnectString(String name) {
    return String.format("jdbc:h2:mem:%s;LOCK_MODE=3", name);
  }

  protected DataSource getDataSource(String uri) {
    return CachingDataSourceProvider.getInstance().getDataSource(uri);
  }

  protected Collection<String> getDatabaseNames() {
    Collection<String> names = new ArrayList<String>();
    names.add(hive.getPartitionDimension().getName());
    for (Node node : hive.getNodes()) {
      names.add(node.getName());
    }
    return names;
  }

  protected PartitionDimension createEmptyPartitionDimension() {
    return new PartitionDimensionImpl(Hive.NEW_OBJECT_ID, getHive().getPartitionDimension().getName(), Types.INTEGER,
      getConnectString(getHiveDatabaseName()), new ArrayList<Resource>());
  }

  protected ResourceImpl createResource() {
    final ResourceImpl resource = new ResourceImpl("FOO", Types.INTEGER, false);
    return resource;
  }

  protected SecondaryIndex createSecondaryIndex() {
    SecondaryIndexImpl index = new SecondaryIndexImpl("FOO", java.sql.Types.VARCHAR);
    index.setResource(createResource());
    return index;
  }

  protected SecondaryIndex createSecondaryIndex(int id) {
    SecondaryIndexImpl index = new SecondaryIndexImpl(id, "FOO", java.sql.Types.VARCHAR);
    index.setResource(createResource());
    return index;
  }

  protected NodeImpl createNode(String name) {
    return new NodeImpl(0, name, name, "", HiveDbDialect.H2);
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface Config {
    String value();
  }
}
