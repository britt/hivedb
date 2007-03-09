package org.hivedb.management.quartz;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Date;

import org.hivedb.management.Migration;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.annotations.Test;

public class MigrationJobTest {
	@Test
	public void testJobDetailCreation() {
		JobDetail detail = MigrationJob.createDetail(
				"aDetail", 
				"aGroup", 
				new Migration(getEmigre(), "dimension", mockOrigin(), mockDestination(), "hiveUri"));
		assertNotNull(detail);
		assertEquals("aDetail", detail.getName());
		assertEquals("aGroup", detail.getGroup());
		assertFalse(detail.isDurable());
		assertFalse(detail.isVolatile());
		assertTrue(detail.requestsRecovery());
	}
	@Test
	public void testMigration() {
		JobDetail migration = MigrationJob.createDetail(
				"aJob", 
				"aGroup", 
				new Migration(getEmigre(),"dimension", mockOrigin(), mockDestination(), "hiveUri"));
		try {
			runJob(migration, delayedTrigger(1), new StdSchedulerFactory());
			Thread.sleep(500); 
			//Quirk of threading in tests, unless you wait a bit the mock gets disposed before
			//its method is called thus violating the expectations and throwing and exception.
		} catch (Exception e) {
			e.printStackTrace();
			fail("Somethign went wrong running the migration job.");
		}
	}
	
	private Object getEmigre() {
		return "emigre";
	}
	
	private String mockDestination() {
		return "destination";
	}
	
	private String mockOrigin() {
		return "origin";
	}
	
	private void runJob(JobDetail job, Trigger trigger, SchedulerFactory factory) throws SchedulerException {
		Scheduler cron = factory.getScheduler();
		cron.start();
		cron.scheduleJob(job, trigger);
	}
	
	private Trigger delayedTrigger(int delay) {
		Trigger trigger = TriggerUtils.makeImmediateTrigger(0, delay);
		trigger.setStartTime(new Date());
		trigger.setName("rightNow!");
		return trigger;
	}
}
