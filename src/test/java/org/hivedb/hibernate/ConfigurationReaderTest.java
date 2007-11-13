package org.hivedb.hibernate;

import java.sql.Types;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class ConfigurationReaderTest extends H2HiveTestCase {
	
	@Test
	public void testGetResourceName() throws Exception {
		assertEquals("WeatherReport", new ConfigurationReader().getResourceName(WeatherReport.class));
	}
	@SuppressWarnings("unchecked")
	@Test
	public void configureResourceTest() throws Exception {
		EntityConfig config = getEntityHiveConfig().getEntityConfig(WeatherReport.class.getName());
		WeatherReport report = WeatherReportImpl.generate();
		assertEquals(WeatherReport.CONTINENT, config.getPrimaryIndexKeyPropertyName());
		assertEquals(WeatherReport.CONTINENT, config.getPartitionDimensionName());
		assertEquals(report.getContinent(), config.getPrimaryIndexKey(report));
		assertEquals(report.getReportId(), config.getId(report));
		assertEquals("WeatherReport", config.getResourceName());
		assertFalse(config.isPartitioningResource());
		assertEquals(WeatherReport.class, config.getRepresentedInterface());
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
	public void testInstall() throws Exception {
		Hive hive = getHive();
		EntityConfig config = new ConfigurationReader().configure(Continent.class);
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(1,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testInstallPartitioningResource() throws Exception {
		Hive hive = getHive();
		EntityConfig config = new ConfigurationReader().configure(Continent.class);
		assertNotNull(hive.getPartitionDimension().getResource(config.getResourceName()));
		assertEquals(1,hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes().size());
		assertEquals(Types.INTEGER,Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getColumnInfo().getColumnType());
		assertEquals("population", Atom.getFirst(hive.getPartitionDimension().getResource(config.getResourceName()).getSecondaryIndexes()).getName());
	}
}
