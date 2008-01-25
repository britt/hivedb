package org.hivedb.services;

import org.hivedb.util.database.test.ClassDaoServiceTest;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReportImpl;
import org.hivedb.util.database.test.WeatherSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ExampleDaoServiceTest extends ClassDaoServiceTest {

	@Override
	@BeforeClass
	public void initializeDataProvider() {
		addEntity(WeatherReportImpl.class, new WeatherSchema(getConnectString(getHiveDatabaseName())));
		addEntity(WeatherEvent.class);
	}
	
	@Test
	public void pluginDetectable(){}

}
