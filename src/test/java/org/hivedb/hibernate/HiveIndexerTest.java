package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveFacade;
import org.hivedb.annotations.IndexType;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityConfigImpl;
import org.hivedb.configuration.EntityIndexConfig;
import org.hivedb.configuration.EntityIndexConfigImpl;
import org.hivedb.meta.Node;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.functional.Validator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveIndexerTest extends H2HiveTestCase {
	@BeforeMethod
	public void setup() throws Exception {
		HiveFacade hive = getHive();
		hive.addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2));
	}

	
	// Test for HiveIndexer.insert(final EntityIndexConfiguration config, final Object entity)
	@Test
	public void insertTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateIntance();
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		for(Integer temperature : report.getWeeklyTemperatures()) 
			assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "weeklyTemperatures", temperature, report.getReportId()));
	}
	
	// Test for HiveIndexer.update(EntityIndexConfiguration config, Object entity)
	@Test
	public void updateTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateIntance();
		Integer oldTemp = report.getTemperature();
		indexer.insert(getWeatherReportConfig(report), report);
		assertEquals(1, hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", report.getContinent()).size());
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		report.setTemperature(32);
		indexer.update(getWeatherReportConfig(report), report);
		assertFalse(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(report.getTemperature()));
	}
	
	@Test
	public void changePartitionKeyTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateIntance();
		report.setContinent("Asia");
		Integer oldTemp = report.getTemperature();
		indexer.insert(getWeatherReportConfig(report), report);
		assertEquals(1, hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", report.getContinent()).size());
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		assertEquals("Asia", hive.directory().getPrimaryIndexKeyOfResourceId("WeatherReport", report.getReportId()));
		
		report.setTemperature(32);
		report.setContinent("Europe");
		indexer.update(getWeatherReportConfig(report), report);
		assertFalse(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(oldTemp));
		assertTrue(hive.directory().getSecondaryIndexKeysWithResourceId("WeatherReport", "temperature", report.getReportId()).contains(report.getTemperature()));
		
		Collection<Object> asiatics = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Asia");
		Collection<Object> europeans = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Europe");
		System.out.println(String.format("There are %s asiatics: %s", asiatics.size(), asiatics));
		System.out.println(String.format("There are %s europeans: %s", europeans.size(), europeans));
		
		assertEquals("Europe", hive.directory().getPrimaryIndexKeyOfResourceId("WeatherReport", report.getReportId()));	
	}


	private WeatherReport generateIntance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
	}
	
	// Test for HiveIndexer.delete(EntityIndexConfiguration config, Object entity)
	@Test
	public void deleteTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateIntance();
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
		WeatherReport report = generateIntance();
		assertFalse(indexer.exists(getWeatherReportConfig(report), report));
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(indexer.exists(getWeatherReportConfig(report), report));
	}
	
	private EntityConfig getWeatherReportConfig(final WeatherReport report) {
		return new EntityConfigImpl(
				WeatherReport.class,
				WeatherReport.CONTINENT,
				"WeatherReport",
				"continent",
				"reportId",
				null,
				Arrays.asList(
						new EntityIndexConfigImpl(WeatherReport.class, "temperature"),
						new EntityIndexConfigImpl(WeatherReport.class, "weeklyTemperatures")),
				false);	
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
			}

			public IndexType getIndexType() {
				// TODO Auto-generated method stub
				return null;
			}

			public Validator getValidator() {
				// TODO Auto-generated method stub
				return null;
			}};   
	}	
	
}
