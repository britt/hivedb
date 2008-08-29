package org.hivedb.hibernate;

import org.hivedb.Hive;
import org.hivedb.configuration.EntityConfig;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.junit.Test;import static org.junit.Assert.assertTrue;import static org.junit.Assert.assertEquals;import static org.junit.Assert.assertFalse;

import java.util.Collection;

@Config(file="hive_default")
public class HiveIndexerTest extends HiveTest {
	
	// Test for HiveIndexer.insert(final EntityIndexConfiguration config, final Object entity)
	@Test
	public void insertTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateInstance();
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
		WeatherReport report = generateInstance();
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
		WeatherReport report = generateInstance();
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
		Collection<Object> asiatics = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Asia");
		Collection<Object> europeans = hive.directory().getResourceIdsOfPrimaryIndexKey("WeatherReport", "Europe");
		
		assertEquals("Europe", hive.directory().getPrimaryIndexKeyOfResourceId("WeatherReport", report.getReportId()));	
	}


	private WeatherReport generateInstance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
	}
	
	// Test for HiveIndexer.delete(EntityIndexConfiguration config, Object entity)
	@Test
	public void deleteTest() throws Exception {
		Hive hive = getHive();
		HiveIndexer indexer = new HiveIndexer(hive);
		WeatherReport report = generateInstance();
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
		WeatherReport report = generateInstance();
		assertFalse(indexer.exists(getWeatherReportConfig(report), report));
		indexer.insert(getWeatherReportConfig(report), report);
		assertTrue(indexer.exists(getWeatherReportConfig(report), report));
	}
	
	private EntityConfig getWeatherReportConfig(final WeatherReport report) {
		return getEntityHiveConfig().getEntityConfig(WeatherReport.class);
	}
}
