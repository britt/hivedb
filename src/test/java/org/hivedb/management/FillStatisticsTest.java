package org.hivedb.management;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.management.statistics.FillStatisticsMBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class FillStatisticsTest {
	ApplicationContext context = null;

	@BeforeTest
	public void setUp() {
		String[] files = new String[] { "/hivedb-jmx.xml" };
		context = new ClassPathXmlApplicationContext(files);
	}

	private static final long FILL = System.currentTimeMillis();
	private static final long CAPACITY = System.currentTimeMillis();
	@Test
	public void verifyFillStatistics() throws Exception {
		FillStatisticsMBean stat1 = (FillStatisticsMBean) context.getBean("fillStatisticsProxy");
		stat1.setFill(FILL);
		stat1.setCapacity(CAPACITY);
		FillStatisticsMBean stat2 = (FillStatisticsMBean) context.getBean("fillStatisticsProxy");
		assertEquals(FILL,stat2.getFill());
		assertEquals(CAPACITY,stat2.getCapacity());
		assertEquals((float)FILL/CAPACITY,stat2.getPercentage());
	}
}
