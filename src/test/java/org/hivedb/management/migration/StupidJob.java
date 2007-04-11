package org.hivedb.management.migration;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class StupidJob implements Job {
	public static boolean didStupidJobRun = false;
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		didStupidJobRun = true;
	}
}
