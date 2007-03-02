package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.assertEquals;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FillStatisticsTest {
	ApplicationContext context = null;

	@BeforeMethod
	public void setUp() {
		
	}

	private static final long FILL = System.currentTimeMillis();
	
	@Test
	public void testFillStatistics() throws Exception {
		String[] files = new String[] { "/hivedb-jmx.xml" };
		context = new ClassPathXmlApplicationContext(files);
		NodeFillStatistics stat1 = (NodeFillStatistics) context.getBean("fillStatisticsProxy");
		stat1.addToFillLevel(FILL);
		NodeFillStatistics stat2 = (NodeFillStatistics) context.getBean("fillStatisticsProxy");
		assertEquals(FILL,stat2.getAverageFillLevel());
	}
}
