package org.hivedb.management;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Date;

import org.quartz.JobDataMap;
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

/**
 * QuartzTest is not a proper unit test. Since quartz spawns a number of child
 * processes its difficult to do proper assertions. Really this is just
 * exploratory code written while learning to use quartz.
 * 
 * @author Britt Crawford (bcrawford@cafepress.com)
 * 
 */
public class QuartzTest {
	private static final String TRIGGER_GROUP = "myTriggers";

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
	public void testQuartzJob() {
		runJob(simpleJobDetail(), delayedTrigger(1), new StdSchedulerFactory());
	}

	@Test
	public void testJobDataMap() {
		JobDetail detail = new JobDetail("aJob", "aGroup",StupidMappedJob.class);
		detail.setVolatility(true);
		detail.setDurability(false);
		JobDataMap map = detail.getJobDataMap();
		map.put("aKey", "aValue");
		detail.setJobDataMap(map);
		runJob(detail, delayedTrigger(1), new StdSchedulerFactory());
	}

	@Test
	public void testJobQueueInspection() {
		try {
			String jobName1 = "ajob #1";
			String groupName = "myGroup";
			JobDetail detail1 = new JobDetail(jobName1, groupName,StupidMappedJob.class);
			String jobName2 = "bjob #2";
			JobDetail detail2 = new JobDetail(jobName2, groupName,StupidMappedJob.class);

			JobDataMap map = detail1.getJobDataMap();
			map.put("JobNumber", "1");
			detail1.setJobDataMap(map);

			JobDataMap map2 = detail2.getJobDataMap();
			map2.put("JobNumber", "2");
			detail2.setJobDataMap(map2);

			Scheduler cron = new StdSchedulerFactory().getScheduler();
			clearJobQueue(cron);

			int first = 1;
			cron.scheduleJob(detail1, delayedTrigger(first));
			int last = 5;
			cron.scheduleJob(detail2, delayedTrigger(last));

			String[] jobNames = cron.getJobNames(groupName);
			assertEquals(2, jobNames.length); // jobStore is persistent across
												// tests
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	private void clearJobQueue(Scheduler cron) throws SchedulerException {
		String[] groups = cron.getJobGroupNames();
		for (String group : groups) {
			String[] jobs = cron.getJobNames(group);
			for (String job : jobs)
				cron.deleteJob(job, group);
		}
	}

	private void runJob(JobDetail job, Trigger trigger, SchedulerFactory factory) {
		Scheduler cron;
		try {
			cron = factory.getScheduler();
			cron.start();
			cron.scheduleJob(job, trigger);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	private Trigger delayedTrigger(int delay) {
		Trigger trigger = TriggerUtils.makeImmediateTrigger(0, delay);
		trigger.setStartTime(new Date());
		trigger.setGroup(TRIGGER_GROUP);
		trigger.setName("rightNow!" + Math.random());
		trigger.setVolatility(false);
		return trigger;
	}

	@SuppressWarnings("serial")
	private JobDetail simpleJobDetail() {
		return new JobDetail("aJob", "myGroup", StupidJob.class);
	}
}
