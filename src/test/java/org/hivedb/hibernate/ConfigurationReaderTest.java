package org.hivedb.hibernate;

import org.hivedb.Hive;
import org.hivedb.Schema;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.HiveConfigurationSchema;
import org.hivedb.management.HiveConfigurationSchemaInstaller;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ConfigurationReaderTest extends H2TestCase {

  @Before
  public void beforeMethod() {
    super.beforeMethod();
    deleteDatabasesAfterEachTest = true;
  }

  public Collection<Schema> getSchemas() {
    return Arrays.asList(new Schema[]{
      new HiveConfigurationSchema(getConnectString(H2TestCase.TEST_DB))});
  }

  @Test
  public void testGetResourceName() throws Exception {
    assertEquals("WeatherReport", ConfigurationReader.getResourceName(WeatherReport.class));
  }

  @Test
  public void configureResourceTest() throws Exception {
    EntityConfig config = new ConfigurationReader().configure(WeatherReport.class);
    WeatherReport report = generateInstance();
    assertEquals(WeatherReport.CONTINENT, config.getPrimaryIndexKeyPropertyName());
    assertEquals(WeatherReport.CONTINENT, config.getPartitionDimensionName());
    assertEquals(report.getContinent(), config.getPrimaryIndexKey(report));
    assertEquals(report.getReportId(), config.getId(report));
    assertEquals("WeatherReport", config.getResourceName());
    assertFalse(config.isPartitioningResource());
    assertEquals(WeatherReport.class, config.getRepresentedInterface());
    assertEquals(Integer.class, config.getIdClass());

    Collection<EntityIndexConfig> indexes = (Collection<EntityIndexConfig>) config.getEntityIndexConfigs();

    EntityIndexConfig temperature = null;
    for (EntityIndexConfig icfg : indexes)
      if ("temperature".equals(icfg.getIndexName())) {
        temperature = icfg;
        break;
      }
    assertNotNull(temperature);
    assertEquals(int.class, temperature.getIndexClass());
    assertEquals(report.getTemperature(), Atom.getFirst(temperature.getIndexValues(report)));

    Filter.grepSingleOrNull(new Predicate<EntityIndexConfig>() {
      public boolean f(EntityIndexConfig entityIndexConfig) {
        return "weatherEventEventId".equals(entityIndexConfig.getIndexName());
      }
    }, indexes);
  }


  private WeatherReport generateInstance() {
    return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
  }

  @Test
  public void configurePartitioningResourceTest() throws Exception {
    EntityConfig config = new ConfigurationReader().configure(Continent.class);
    Continent asia = new AsiaticContinent();
    assertEquals("name", config.getPrimaryIndexKeyPropertyName());
    assertEquals(WeatherReport.CONTINENT, config.getPartitionDimensionName());
    assertEquals("Asia", config.getPrimaryIndexKey(asia));
    assertEquals("Asia", config.getId(asia));
    assertEquals(WeatherReport.CONTINENT, config.getResourceName());
    assertTrue(config.isPartitioningResource());
    assertEquals(Continent.class, config.getRepresentedInterface());
    assertEquals(String.class, config.getIdClass());

    Collection<EntityIndexConfig> indexes = (Collection<EntityIndexConfig>) config.getEntityIndexConfigs();
    assertEquals(1, indexes.size());
    assertEquals("population", Atom.getFirst(indexes).getIndexName());
    assertEquals(Integer.class, Atom.getFirst(indexes).getIndexClass());
    assertEquals(asia.getPopulation(), Atom.getFirst(Atom.getFirst(indexes).getIndexValues(asia)));
  }

  @Test
  public void testInstallWithPartitionDimensionInstalled() throws Exception {
    new HiveConfigurationSchemaInstaller(getConnectString(H2TestCase.TEST_DB)).run();
    Hive hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReport.CONTINENT, Types.VARCHAR, org.hivedb.meta.persistence.CachingDataSourceProvider.getInstance(), null);
    ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
    reader.install(hive);
    EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
    assertEquals(hiveEntityIndexConfigCount, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
  }

  @Test
  public void testInstallPartitioningResourceWithPartitionDimensionInstalled() throws Exception {
    new HiveConfigurationSchemaInstaller(getConnectString(H2TestCase.TEST_DB)).run();
    Hive hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReport.CONTINENT, Types.VARCHAR, CachingDataSourceProvider.getInstance(), null);
    ConfigurationReader reader = new ConfigurationReader(Continent.class);
    reader.install(hive);
    EntityConfig config = reader.getEntityConfig(Continent.class.getName());
    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    assertEquals(1, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
    assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
  }

  @Test
  public void testInstallWithoutHiveSchema() throws Exception {
    ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
    reader.install(getConnectString(H2TestCase.TEST_DB));
    EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
    Hive hive = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
    assertEquals(hiveEntityIndexConfigCount, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
  }

  @Test
  public void testInstallPartitioningResourceWithoutHiveSchema() throws Exception {
    ConfigurationReader reader = new ConfigurationReader(Continent.class);
    reader.install(getConnectString(H2TestCase.TEST_DB));
    EntityConfig config = reader.getEntityConfig(Continent.class.getName());
    Hive hive = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());

    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    assertEquals(1, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
    assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
  }

  @Test
  public void testInstallWithoutPartitionDimension() throws Exception {
    new HiveConfigurationSchemaInstaller(getConnectString(H2TestCase.TEST_DB)).run();
    ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
    reader.install(getConnectString(H2TestCase.TEST_DB));
    EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
    Hive hive = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());
    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
    assertEquals(hiveEntityIndexConfigCount, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
  }

  @Test
  public void testInstallPartitioningResourceWithoutPartitionDimension() throws Exception {
    new HiveConfigurationSchemaInstaller(getConnectString(H2TestCase.TEST_DB)).run();
    ConfigurationReader reader = new ConfigurationReader(Continent.class);
    reader.install(getConnectString(H2TestCase.TEST_DB));
    EntityConfig config = reader.getEntityConfig(Continent.class.getName());
    Hive hive = Hive.load(getConnectString(H2TestCase.TEST_DB), CachingDataSourceProvider.getInstance());

    assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
    assertEquals(1, hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
    assertEquals(Types.INTEGER, Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
    assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
  }

  @Override
  public Collection<String> getDatabaseNames() {
    return Collections.singletonList(H2TestCase.TEST_DB);
  }
}
