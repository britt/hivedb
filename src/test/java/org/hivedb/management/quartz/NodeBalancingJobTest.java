package org.hivedb.management.quartz;

import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Date;

import org.hivedb.meta.Node;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class NodeBalancingJobTest {
	@BeforeClass()
	public void setUp() {
		System.setProperty("org.quartz.jobStore.class",
				"org.quartz.simpl.RAMJobStore");
	}

	@AfterClass()
	public void tearDown() {
		System.clearProperty("org.quartz.jobStore.class");
	}
	
	@Test
	public void testExecution() {
		JobDetail balance = NodeBalancingJob.createDetail(
				"aJob", 
				"aGroup", 
				new ArrayList<Node>());
		try {
			runJob(balance, delayedTrigger(1), new StdSchedulerFactory());
			Thread.sleep(500); 
			//Quirk of threading in tests, unless you wait a bit the mock gets disposed before
			//its method is called thus violating the expectations and throwing and exception.
		} catch (Exception e) {
			e.printStackTrace();
			fail("Somethign went wrong running the migration job.");
		}
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
