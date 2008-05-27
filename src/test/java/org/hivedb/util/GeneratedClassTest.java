package org.hivedb.util;

import org.apache.cxf.aegis.databinding.AegisDatabinding;
import org.apache.cxf.jaxb.JAXBDataBinding;
import org.hivedb.util.classgen.GenerateInstance;
import org.hivedb.util.database.test.WeatherReport;
import org.testng.annotations.Test;

public class GeneratedClassTest {
	
	@Test
	public void testGetClass() throws Exception {
		WeatherReport instance = new GenerateInstance<WeatherReport>(WeatherReport.class).generate();
		instance.getClass().newInstance();
	}
}
