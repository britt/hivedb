package org.hivedb.management;

import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.sql.ResultSet;
import java.util.Date;

import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.DerbyUtils;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class QuartzJdbcJobStoreTest extends DerbyTestCase {
	public QuartzJdbcJobStoreTest() {
		databaseName = "quartz";
		StringBuilder sqlScript = new StringBuilder("src");
		sqlScript.append(File.separator);
		sqlScript.append("main");
		sqlScript.append(File.separator);
		sqlScript.append("resources");
		sqlScript.append(File.separator);
		sqlScript.append("create_quartz_tables_derby.sql");
		loadScript = sqlScript.toString();
	}
	
	private void configureQuartzJobStore() {
		System.setProperty("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
		System.setProperty("org.quartz.jobStore.driverDelegateClass", "org.quartz.impl.jdbcjobstore.CloudscapeDelegate");
		System.setProperty("org.quartz.jobStore.tablePrefix", "qrtz_");

		//Use a Quartz managed DataSource
		System.setProperty("org.quartz.jobStore.dataSource", "quartzDS");
		System.setProperty("org.quartz.dataSource.quartzDS.driver", DerbyUtils.derbyDriver);
		System.setProperty("org.quartz.dataSource.quartzDS.URL", DerbyUtils.connectString(databaseName));
		System.setProperty("org.quartz.dataSource.quartzDS.user", userName);
		System.setProperty("org.quartz.dataSource.quartzDS.password", password);
		System.setProperty("org.quartz.dataSource.quartzDS.maxConenctions", "10");
	}
	
	@BeforeClass
	public void setUp() {
		configureQuartzJobStore();
	}
	
	@AfterClass
	public void tearDown() {		
		System.clearProperty("org.quartz.jobStore.class");
		System.clearProperty("org.quartz.jobStore.driverDelegateClass");
		System.clearProperty("org.quartz.jobStore.tablePrefix");

		//Use a Quartz managed DataSource
		System.clearProperty("org.quartz.jobStore.dataSource");
		System.clearProperty("org.quartz.dataSource.quartzDS.driver");
		System.clearProperty("org.quartz.dataSource.quartzDS.URL");
		System.clearProperty("org.quartz.dataSource.quartzDS.user");
		System.clearProperty("org.quartz.dataSource.quartzDS.password");
		System.clearProperty("org.quartz.dataSource.quartzDS.maxConenctions");
	}
	
	@Test
	public void testQuartzJob() {
		SchedulerFactory factory = new StdSchedulerFactory();
		Scheduler cron;
		try {
			cron = factory.getScheduler();
			cron.start();
			cron.scheduleJob(simpleJobDetail(), delayedTrigger(1));
			ResultSet results = getConnection().createStatement().executeQuery("select * from qrtz_job_details");
			int rowCount = 0;
			while(results.next())
				rowCount++;
			//assertTrue(rowCount > 0);
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception while running the quartz job. " + e.getMessage());
		}
	}

	private Trigger delayedTrigger(int delay) {
		Trigger trigger = TriggerUtils.makeImmediateTrigger(0, delay);
		trigger.setStartTime(new Date());
		trigger.setName("rightNow!");
		return trigger;
	}
	
	@SuppressWarnings("serial")
	private JobDetail simpleJobDetail(){
		return new JobDetail("aJob","myGroup",StupidJob.class);
	}
	

}
