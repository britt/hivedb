package org.hivedb.hibernate;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.management.HiveInstaller;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.functional.Filter;
import org.hivedb.util.functional.Predicate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class ConfigurationReaderTest extends H2TestCase {
	
	@Test
	public void testGetResourceName() throws Exception {
		assertEquals("WeatherReport", new ConfigurationReader().getResourceName(WeatherReport.class));
	}
	
	@SuppressWarnings("unchecked")
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
		for(EntityIndexConfig icfg : indexes)
			if("temperature".equals(icfg.getIndexName())){
				temperature = icfg;
				break;
			}
		assertNotNull(temperature);
		assertEquals(int.class, temperature.getIndexClass());
		assertEquals(report.getTemperature(), Atom.getFirst(temperature.getIndexValues(report)));
		
		Filter.grepSingleOrNull(new Predicate<EntityIndexConfig>() {
			public boolean f(EntityIndexConfig entityIndexConfig) {
				return "weatherEventEventId".equals(entityIndexConfig.getIndexName());
			}}, indexes);
	}

	private WeatherReport generateInstance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
	}
	
	@SuppressWarnings("unchecked")
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
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallWithPartitionDimensionInstalled() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		HiveFacade hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReport.CONTINENT, Types.VARCHAR);
		ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
		reader.install(hive);
		EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
		assertEquals(hiveEntityIndexConfigCount,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallPartitioningResourceWithPartitionDimensionInstalled() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		HiveFacade hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReport.CONTINENT, Types.VARCHAR);
		ConfigurationReader reader = new ConfigurationReader(Continent.class);
		reader.install(hive);
		EntityConfig config = reader.getEntityConfig(Continent.class.getName());
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(1,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallWithoutHiveSchema() throws Exception {
		ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
		assertEquals(hiveEntityIndexConfigCount,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallPartitioningResourceWithoutHiveSchema() throws Exception {
		ConfigurationReader reader = new ConfigurationReader(Continent.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(Continent.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(1,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallWithoutPartitionDimension() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		ConfigurationReader reader = new ConfigurationReader(WeatherReport.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(WeatherReport.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		int hiveEntityIndexConfigCount = config.getEntityIndexConfigs(IndexType.Hive).size();
		assertEquals(hiveEntityIndexConfigCount,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallPartitioningResourceWithoutPartitionDimension() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		ConfigurationReader reader = new ConfigurationReader(Continent.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(Continent.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(1,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}

	@Override
	public Collection<String> getDatabaseNames() {
		return Collections.singletonList(H2HiveTestCase.TEST_DB);
	}
}
