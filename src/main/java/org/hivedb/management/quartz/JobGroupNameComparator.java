package org.hivedb.management.quartz;

import java.util.Comparator;

import org.quartz.JobDetail;

public class JobGroupNameComparator implements Comparator<JobDetail> {

	public int compare(JobDetail o1, JobDetail o2) {
		return o1.getGroup().compareTo(o2.getGroup());
	}

}
