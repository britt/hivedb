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
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.validators.Validator;
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
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		for(WeatherEvent weatherEvent : report.getWeatherEvents()) 
			assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "weatherEventEventId", weatherEvent.getEventId(), report.getReportId()));
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
		assertTrue(hive.directory().getResourceIdsOfPrimaryIndexKey("Temperature", report.getContinent()).contains(oldTemp));
		GeneratedInstanceInterceptor.setProperty(report, "temperature", 32);
		indexer.update(getWeatherReportConfig(report), report);
		assertTrue(hive.directory().getResourceIdsOfPrimaryIndexKey("Temperature", report.getContinent()).contains(report.getTemperature()));
	}
	
	@Test
	public void changePartitionKeyTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateIntance();
		GeneratedInstanceInterceptor.setProperty(report, "continent", "Asia");
		Integer oldTemp = report.getTemperature();
		indexer.insert(getWeatherReportConfig(report), report);
		assertEquals(1, hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", report.getContinent()).size());
		assertTrue(hive.directory().getResourceIdsOfPrimaryIndexKey("Temperature", report.getContinent()).contains(oldTemp));
		assertEquals("Asia", hive.directory().getPrimaryIndexKeyOfResourceId("WeatherReport", report.getReportId()));
		
		GeneratedInstanceInterceptor.setProperty(report, "continent", "Europe");
		indexer.update(getWeatherReportConfig(report), report);
		GeneratedInstanceInterceptor.setProperty(report, "temperature", 32);
		indexer.update(getWeatherReportConfig(report), report);
		assertTrue(hive.directory().getResourceIdsOfPrimaryIndexKey("Temperature", report.getContinent()).contains(report.getTemperature()));
		Collection<Object> asiatics = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Asia");
		Collection<Object> europeans = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Europe");
		
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
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		indexer.delete(getWeatherReportConfig(report), report);
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		// Temperature is an entity so it does not get deleted
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
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
		return getEntityHiveConfig().getEntityConfig(WeatherReport.class);
	}
}
