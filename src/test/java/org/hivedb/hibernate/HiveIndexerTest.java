package org.hivedb.hibernate;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.meta.Node;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.*;
import static org.testng.AssertJUnit.*;

public class HiveIndexerTest extends H2HiveTestCase {
	@BeforeMethod
	public void setup() throws Exception {
		Hive hive = Hive.create(getConnectString(getHiveDatabaseName()), WeatherReport.CONTINENT, Types.VARCHAR);
		Resource resource = new Resource("WeatherReport",Types.INTEGER,false);
		hive.addResource(resource);
		hive.addSecondaryIndex(resource, new SecondaryIndex("temperature", Types.INTEGER));
		hive.addSecondaryIndex(resource, new SecondaryIndex("collectionIndex", Types.INTEGER));
		hive.addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", hive.getPartitionDimension().getId(), HiveDbDialect.H2));
	}

	
	// Test for HiveIndexer.insert(final EntityIndexConfiguration config, final Object entity)
	@Test
	public void insertTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = WeatherReport.generate();
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		for(Integer i : report.getCollectionIndex()) 
			assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "collectionIndex", i, report.getReportId()));
	}
	
	// Test for HiveIndexer.update(EntityIndexConfiguration config, Object entity)
	@Test
	public void updateTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = WeatherReport.generate();
		Integer oldTemp = report.getTemperature();
		indexer.insert(getWeatherReportConfig(report), report);
		assertEquals(1, hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", report.getContinent()).size());
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		report.setTemperature(32);
		indexer.update(getWeatherReportConfig(report), report);
		assertFalse(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(report.getTemperature()));
	}
	
	// Test for HiveIndexer.delete(EntityIndexConfiguration config, Object entity)
	@Test
	public void deleteTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = WeatherReport.generate();
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		indexer.delete(getWeatherReportConfig(report), report);
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertFalse(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		indexer.delete(getWeatherReportConfig(report), report);
	}
	
	// Test for HiveIndexer.exists(EntityIndexConfiguration config, Object entity)
	@Test
	public void existsTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = WeatherReport.generate();
		assertFalse(indexer.exists(getWeatherReportConfig(report), report));
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(indexer.exists(getWeatherReportConfig(report), report));
	}
	
	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
	
	private EntityConfig getWeatherReportConfig(final WeatherReport report) {
		return new EntityConfig() {
			public Collection<? extends EntityIndexConfig> getEntitySecondaryIndexConfigs() {
				return Arrays.asList(
						new EntityIndexConfigImpl(WeatherReport.class, "temperature"),
						new EntityIndexConfigImpl(WeatherReport.class, "collectionIndex"));
			}

			public Integer getId(Object instance) {
				return ((WeatherReport) instance).getReportId();
			}

			public Class<?> getIdClass() {
				return Integer.class;
			}

			public String getIdPropertyName() {
				return "reportId";
			}

			public String getPartitionDimensionName() {
				return WeatherReport.CONTINENT;
			}

			public Object getPrimaryIndexKey(Object instance) {
				return ((WeatherReport) instance).getContinent();
			}

			public String getPrimaryIndexKeyPropertyName() {
				return "continent";
			}

			public Class getRepresentedInterface() {
				return WeatherReport.class;
			}

			public String getResourceName() {
				return "WeatherReport";
			}

			public boolean isPartitioningResource() {
				return false;
			}};
		}


	private EntityIndexConfig dummyIndexConfig(final String name, final Class clazz, final Object value) {
		return new EntityIndexConfig(){

			public Class<?> getIndexClass() {
				return clazz;
			}

			public String getIndexName() {
				return name;
			}

			public Collection<Object> getIndexValues(Object entityInstance) {
				return Arrays.asList(value);
			}

			public String getPropertyName() {
				return name;
			}};
	}	
	
}
