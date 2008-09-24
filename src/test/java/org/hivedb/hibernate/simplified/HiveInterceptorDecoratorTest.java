package org.hivedb.hibernate.simplified;

import org.hibernate.CallbackException;
import org.hivedb.Hive;
import org.hivedb.HiveLockableException;
import org.hivedb.Lockable;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.hibernate.AsiaticContinent;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.hibernate.HiveIndexer;
import org.hivedb.hibernate.HiveInterceptorDecorator;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.classgen.GeneratedInstanceInterceptor;
import org.hivedb.util.database.test.Continent;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.WeatherReport;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;

@HiveTest.Config("hive_default")
public class HiveInterceptorDecoratorTest extends HiveTest {
	
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
		hive.directory().insertPrimaryIndexKey(asia.getName());
		hive.directory().insertPrimaryIndexKey(report.getContinent());
		interceptor.postFlush(Arrays.asList(new Object[]{report,asia}).iterator());

		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));

		assertTrue(hive.directory().doesPrimaryIndexKeyExist(asia.getName()));
		assertTrue(hive.directory().doesSecondaryIndexKeyExist("Continent", "population", asia.getPopulation(), asia.getName()));

	}

	@Test
	public void testOnSaveInsertReadOnlyFailure() throws Exception {
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);

		WeatherReport report = generateInstance();
		hive.directory().insertPrimaryIndexKey(report.getContinent());
		hive.updateHiveStatus(Lockable.Status.readOnly);
		try {
			interceptor.postFlush(Arrays.asList(new Object[]{report}).iterator());
			fail("No exception thrown");
		} catch(CallbackException e ) {
			assertEquals(HiveLockableException.class, e.getCause().getClass());
		}
	}

	@Test
	public void testOnDeleteReadOnlyFailure() throws Exception{
		EntityHiveConfig config = getEntityHiveConfig();
		Hive hive = getHive();

		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);

		WeatherReport report = generateInstance();
		hive.directory().insertPrimaryIndexKey(report.getContinent());
		interceptor.postFlush(Arrays.asList(new WeatherReport[]{report}).iterator());

		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));

		hive.updateHiveStatus(Lockable.Status.readOnly);
		try {
			interceptor.onDelete(report, null, null, null, null);
			fail("No exception thrown");
		} catch(CallbackException e ) {
			assertEquals(HiveLockableException.class, e.getCause().getClass());
		}
	}

	@Test
	public void testOnDelete() throws Exception{
		Hive hive = getHive();
		ConfigurationReader reader = new ConfigurationReader(Continent.class, WeatherReport.class);
		EntityHiveConfig config = reader.getHiveConfiguration();
		HiveInterceptorDecorator interceptor = new HiveInterceptorDecorator(config, hive);

		WeatherReport report = generateInstance();
		Continent asia = new AsiaticContinent();
		hive.directory().insertPrimaryIndexKey(report.getContinent());
		hive.directory().insertPrimaryIndexKey(asia.getName());
		interceptor.postFlush(Arrays.asList(new Object[]{report,asia}).iterator());

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
		hive.directory().insertPrimaryIndexKey(report.getContinent());
		interceptor.postFlush(Arrays.asList(new WeatherReport[]{report}).iterator());

		assertTrue(hive.directory().doesPrimaryIndexKeyExist(report.getContinent()));
		assertTrue(hive.directory().doesResourceIdExist("WeatherReport", report.getReportId()));
		assertTrue(hive.directory().doesResourceIdExist("Temperature", report.getTemperature()));

		int oldTemperature = report.getTemperature();
		GeneratedInstanceInterceptor.setProperty(report, "temperature", 72);
		assertFalse(oldTemperature == 72);
		interceptor.postFlush(Arrays.asList(new WeatherReport[]{report}).iterator());
		assertTrue(hive.directory().doesResourceIdExist("Temperature", 72));
	}
}


