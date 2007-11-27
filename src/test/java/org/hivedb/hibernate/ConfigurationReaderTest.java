package org.hivedb.hibernate;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.management.HiveInstaller;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class ConfigurationReaderTest extends H2TestCase {
	
	@Test
	public void testGetResourceName() throws Exception {
		assertEquals("WeatherReport", new ConfigurationReader().getResourceName(WeatherReportImpl.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void configureResourceTest() throws Exception {
		EntityConfig config = new ConfigurationReader().configure(WeatherReportImpl.class);
		WeatherReport report = WeatherReportImpl.generate();
		assertEquals(WeatherReport.CONTINENT, config.getPrimaryIndexKeyPropertyName());
		assertEquals(WeatherReport.CONTINENT, config.getPartitionDimensionName());
		assertEquals(report.getContinent(), config.getPrimaryIndexKey(report));
		assertEquals(report.getReportId(), config.getId(report));
		assertEquals("WeatherReport", config.getResourceName());
		assertFalse(config.isPartitioningResource());
		assertEquals(WeatherReportImpl.class, config.getRepresentedInterface());
		assertEquals(Integer.class, config.getIdClass());
		
		Collection<EntityIndexConfig> indexes = (Collection<EntityIndexConfig>) config.getEntitySecondaryIndexConfigs();
		assertEquals(2, indexes.size());
		
		EntityIndexConfig temperature = null;
		for(EntityIndexConfig icfg : indexes)
			if("temperature".equals(icfg.getIndexName())){
				temperature = icfg;
				break;
			}
		assertNotNull(temperature);
		assertEquals(int.class, temperature.getIndexClass());
		assertEquals(report.getTemperature(), Atom.getFirst(temperature.getIndexValues(report)));
		
		EntityIndexConfig collection = null;
		for(EntityIndexConfig icfg : indexes) {
			if("collectionIndex".equals(icfg.getIndexName())){
				collection = icfg;
				break;
			}
		}
		assertNotNull(collection);
		assertEquals(report.getCollectionIndex(), collection.getIndexValues(report));
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
		
		Collection<EntityIndexConfig> indexes = (Collection<EntityIndexConfig>) config.getEntitySecondaryIndexConfigs();
		assertEquals(1, indexes.size());
		assertEquals("population", Atom.getFirst(indexes).getIndexName());
		assertEquals(Integer.class, Atom.getFirst(indexes).getIndexClass());
		assertEquals(asia.getPopulation(), Atom.getFirst(Atom.getFirst(indexes).getIndexValues(asia)));
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallWithPartitionDimensionInstalled() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		HiveFacade hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReportImpl.CONTINENT, Types.VARCHAR);
		ConfigurationReader reader = new ConfigurationReader(WeatherReportImpl.class);
		reader.install(hive);
		EntityConfig config = reader.getEntityConfig(WeatherReportImpl.class.getName());
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(2,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("temperature", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallPartitioningResourceWithPartitionDimensionInstalled() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		HiveFacade hive = Hive.create(getConnectString(H2TestCase.TEST_DB), WeatherReportImpl.CONTINENT, Types.VARCHAR);
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
		ConfigurationReader reader = new ConfigurationReader(WeatherReportImpl.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(WeatherReportImpl.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(2,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("temperature", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
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
		ConfigurationReader reader = new ConfigurationReader(WeatherReportImpl.class);
		reader.install(getConnectString(H2HiveTestCase.TEST_DB));
		EntityConfig config = reader.getEntityConfig(WeatherReportImpl.class.getName());
		Hive hive = Hive.load(getConnectString(H2HiveTestCase.TEST_DB));
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(2,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("temperature", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
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
