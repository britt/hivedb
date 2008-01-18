package org.hivedb.management;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.mail.Store;

import org.hivedb.Hive;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.testng.annotations.Test;

public class TestTerraFormingHiveFactory extends H2TestCase {
	private static final String HIVE = "hive";

	@Test
	public void barrenHiveDb() throws Exception {
		Hive hive = TerraformingHiveFactory.colonize(getConnectString(HIVE), getPersistedClasses());
		assertNotNull(hive);
		assertNotNull(hive.getPartitionDimension());
		assertNotNull(hive.getPartitionDimension().getResource("WeatherReport"));
	}
	
	@Test
	public void hiveWithSemaphore() throws Exception {
		new HiveInstaller(getConnectString(HIVE)).run();
		barrenHiveDb();
	}
	
	@Test
	public void fullyInstalledHive() throws Exception {
		new HiveInstaller(getConnectString(HIVE)).run();
		new ConfigurationReader(getPersistedClasses()).install(getConnectString(HIVE));
		barrenHiveDb();
	}

	private List<Class<?>> getPersistedClasses() {
		return Arrays.asList(new Class<?>[]{WeatherReport.class, WeatherEvent.class});
	}

	@Override
	public Collection<String> getDatabaseNames() {
		return Collections.singletonList(HIVE);
	}
	
}
