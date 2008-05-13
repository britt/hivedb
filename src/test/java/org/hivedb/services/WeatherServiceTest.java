 package org.hivedb.services;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.mail.Store;

import junit.framework.Assert;

import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Schema;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.test.ClassServiceTest;
import org.hivedb.util.GenerateInstance;
import org.hivedb.util.GeneratedInstanceInterceptor;
import org.hivedb.util.GeneratedServiceInterceptor;
import org.hivedb.util.Lists;
import org.hivedb.util.database.test.WeatherEvent;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherSchema;
import org.hivedb.util.functional.Atom;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups={"service"})
public class WeatherServiceTest extends ClassServiceTest<WeatherReport,WeatherReportService> {

	private static final String WEATHER_SERVICE_URL = "local://weather";
	
	public WeatherServiceTest() {
		super(WeatherReport.class, WeatherReportService.class, WeatherServiceResponse.class, WeatherServiceContainer.class, WEATHER_SERVICE_URL);
	}
	
	@BeforeClass
	public void setup() throws Exception {
		super.setup();
		Assert.assertTrue(server instanceof WeatherReportService);
		final ServiceResponse createServiceResponse = getPersistentInstanceAsServiceResponse(); 
		WeatherServiceContainer weatherReportContainer = (WeatherServiceContainer) Atom.getFirstOrThrow(createServiceResponse.getContainers());
		Assert.assertNotNull(weatherReportContainer.getWeatherReport());
	}
	
	protected Collection<Schema> getSchemata() {
		return Arrays.asList(
				new Schema[]{
						WeatherSchema.getInstance(),
				});
	}

	protected List<Class<?>> getEntityClasses() {
		return Arrays.asList(new Class<?>[]{
				WeatherReport.class});
	}
	
	protected Service createService(ConfigurationReader reader) {
		return GeneratedServiceInterceptor.load(WeatherReport.class, WeatherReportService.class, WeatherServiceResponse.class, WeatherServiceContainer.class, hive, reader.getHiveConfiguration(), getAccessStrategy());
	}

	private SequentialShardAccessStrategy getAccessStrategy() {
		return new SequentialShardAccessStrategy();
	}
}
