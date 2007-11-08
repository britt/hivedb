package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.*;

import java.sql.Types;

import org.hibernate.CallbackException;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveInterceptorDecoratorTest extends H2HiveTestCase {
	
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.create(getConnectString(getHiveDatabaseName()), WeatherReport.CONTINENT, Types.VARCHAR);
		ConfigurationReader config = new ConfigurationReader(Continent.class, WeatherReport.class);
		config.install(hive);
		hive.addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
	}
	
	@Test
	public void testIsTransient() throws Exception{
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		Continent asia = new AsiaticContinent();
		
		assertNotNull(config.getEntityConfig(asia.getClass()));
		
		assertTrue(interceptor.isTransient(report));
		assertTrue(interceptor.isTransient(asia));
		
		HiveIndexer indexer = new HiveIndexer(hive);
		indexer.insert(config.getEntityConfig(WeatherReport.class), report);
		indexer.insert(config.getEntityConfig(Continent.class), asia);
		
		assertFalse(interceptor.isTransient(report));
		assertFalse(interceptor.isTransient(asia));
	}
	
	@Test
	public void testOnSaveInsert() throws Exception {
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		Continent asia = new AsiaticContinent();
		interceptor.onSave(report, null, null, null, null);
		interceptor.onSave(asia, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));
		
	}
	
	@Test
	public void testOnSaveInsertReadOnlyFailure() throws Exception {
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		hive.updateHiveReadOnly(true);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		
		try {
			interceptor.onSave(report, null, null, null, null);
			fail("No exception thrown");
		} catch(CallbackException e ) {
			assertEquals(HiveReadOnlyException.class, e.getCause().getClass());
		}
	}
	
	@Test
	public void testOnDelete() throws Exception{
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		interceptor.onSave(report, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		
		config.getHive().updateHiveReadOnly(true);
		try {
			interceptor.onDelete(report, null, null, null, null);
			fail("No exception thrown");
		} catch(CallbackException e ) {
			assertEquals(HiveReadOnlyException.class, e.getCause().getClass());
		}
	}
	
	@Test
	public void testOnDeleteReadOnlyFailure() throws Exception{
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		Continent asia = new AsiaticContinent();
		interceptor.onSave(report, null, null, null, null);
		interceptor.onSave(asia, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));

		interceptor.onDelete(report, null, null, null, null);
		interceptor.onDelete(asia, null, null, null, null);
		
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertFalse(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		
		assertFalse(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertFalse(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));
	
	}
	
	@Test
	public void testOnSaveUpdate() throws Exception {
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration(hive);
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config);
		
		WeatherReport report = WeatherReport.generate();
		interceptor.onSave(report, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", report.getTemperature(), report.getReportId()));
		
		int oldTemperature = report.getTemperature();
		report.setTemperature(72);
		interceptor.onFlushDirty(report, null, null, null, null,null);
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", 72, report.getReportId()));
		assertFalse(hive.directory().doesSecondaryIndexKeyExist("WeatherReport", "temperature", oldTemperature, report.getReportId()));
	}

	private Hive getHive() {
		return Hive.load(getConnectString(getHiveDatabaseName()));
	}
}
