package org.hivedb.hibernate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.hibernate.CallbackException;
import org.hibernate.EntityMode;
import org.hivedb.Hive;
import org.hivedb.HiveReadOnlyException;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.WeatherReport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveInterceptorDecoratorTest extends H2HiveTestCase {
	
	@BeforeMethod
	public void setUp() throws Exception {
		getHive().addNode(new Node(Hive.NEW_OBJECT_ID, "node", getHiveDatabaseName(), "", HiveDbDialect.H2));
	}
	
	@Test
	public void testInstantiate() throws Exception {
		EntityHiveConfig config = getEntityHiveConfig();
		Hive hive = getHive();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		Object o = interceptor.instantiate("org.hivedb.util.database.test.WeatherReport", EntityMode.POJO, 7);
		assertEquals(GeneratedInstanceInterceptor.getGeneratedClass(WeatherReport.class), o.getClass());
		Object s = interceptor.instantiate("java.lang.String", EntityMode.POJO, 7);
		assertEquals(String.class, s.getClass());
	}
	
	@Test
	public void testIsTransient() throws Exception{
		EntityHiveConfig config = getEntityHiveConfig();
		Hive hive = getHive();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		Continent asia = new AsiaticContinent();
		
		assertNotNull(config.getEntityConfig(asia.getClass()));
		
		assertTrue(interceptor.isTransient(report));
		assertTrue(interceptor.isTransient(asia));
		
		HiveIndexer indexer = new HiveIndexer(getHive());
		indexer.insert(config.getEntityConfig(WeatherReport.class), report);
		indexer.insert(config.getEntityConfig(Continent.class), asia);
		
		assertFalse(interceptor.isTransient(report));
		assertFalse(interceptor.isTransient(asia));
	}

	private WeatherReport generateInstance() {
		return new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
	}
	
	@Test
	public void testOnSaveInsert() throws Exception {
		EntityHiveConfig config = getEntityHiveConfig();
		Hive hive = getHive();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		Continent asia = new AsiaticContinent();
		interceptor.onSave(report, null, null, null, null);
		interceptor.onSave(asia, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));
		
	}
	
	@Test
	public void testOnSaveInsertReadOnlyFailure() throws Exception {
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		hive.updateHiveReadOnly(true);
		EntityHiveConfig config = reader.getHiveConfiguration();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		
		try {
			interceptor.onSave(report, null, null, null, null);
			fail("No exception thrown");
		} catch(CallbackException e ) {
			assertEquals(HiveReadOnlyException.class, e.getCause().getClass());
		}
	}
	
	@Test
	public void testOnDelete() throws Exception{
		EntityHiveConfig config = getEntityHiveConfig();
		Hive hive = getHive();

		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		interceptor.onSave(report, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		
		hive.updateHiveReadOnly(true);
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
		EntityHiveConfig config = reader.getHiveConfiguration();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		Continent asia = new AsiaticContinent();
		interceptor.onSave(report, null, null, null, null);
		interceptor.onSave(asia, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));

		interceptor.onDelete(report, null, null, null, null);
		interceptor.onDelete(asia, null, null, null, null);
		
		assertFalse(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		// Referenced entity does not get deleted
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		
		assertFalse(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertFalse(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));
	
	}
	
	@Test
	public void testOnSaveUpdate() throws Exception {
		Hive hive = getHive();
		EntityHiveConfig config = getEntityHiveConfig();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);
		
		WeatherReport report = generateInstance();
		interceptor.onSave(report, null, null, null, null);
		
		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));
		
		int oldTemperature = report.getTemperature();
		GeneratedInstanceInterceptor.setProperty(report, "temperature", 72);
		interceptor.onFlushDirty(report, null, null, null, null,null);
		assertTrue(hive.directory().doesResourceIdExist("Temperature", 72));
	}
}
