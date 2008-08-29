package org.hivedb.util;

import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.database.test.WeatherReport;
import org.junit.Test;

public class GeneratedClassTest {
	
	@Test
	public void testGetClass() throws Exception {
		WeatherReport instance = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
		instance.getClass().newInstance();
	}
}
