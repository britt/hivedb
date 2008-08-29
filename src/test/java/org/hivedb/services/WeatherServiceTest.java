 package org.hivedb.services;

 import org.junit.Assert;
import org.hibernate.shards.strategy.access.SequentialShardAccessStrategy;
import org.hivedb.Schema;
import org.hivedb.hibernate.ConfigurationReader;
import org.hivedb.test.ClassServiceTest;
import org.hivedb.util.classgen.GeneratedServiceInterceptor;
import org.hivedb.util.database.test.HiveTest.Config;
import org.hivedb.util.database.test.WeatherReport;
import org.hivedb.util.database.test.WeatherSchema;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Config(file="hive_default")
public class WeatherServiceTest extends ClassServiceTest<WeatherReport,WeatherReportService> {

	private static final String WEATHER_SERVICE_URL = "local://weather";
	
	public WeatherServiceTest() {
		super(WeatherReport.class, WeatherReportService.class, WeatherServiceResponse.class, WeatherServiceContainer.class, WEATHER_SERVICE_URL);
	}
	
	public void setup() {
		super.setup();
		Assert.assertTrue(server instanceof WeatherReportService);
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
		return GeneratedServiceInterceptor.load(WeatherReport.class, WeatherReportService.class, WeatherServiceResponse.class, WeatherServiceContainer.class, getHive(), reader.getHiveConfiguration(), getAccessStrategy());
	}

	private SequentialShardAccessStrategy getAccessStrategy() {
		return new SequentialShardAccessStrategy();
	}
}
