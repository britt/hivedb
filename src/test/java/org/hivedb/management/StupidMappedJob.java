package org.hivedb.management;
import java.util.Hashtable;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class StupidMappedJob implements Job {
	Hashtable<Object, Object> dataMap = new Hashtable<Object, Object>();
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobDataMap map = context.getMergedJobDataMap();
		for (Object key : map.keySet())
			dataMap.put(key, map.get(key));
	}
}
