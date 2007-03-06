package org.hivedb.management.statistics;

import java.util.Arrays;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class HivePerformanceStatisticsMBean extends StandardMBean implements
		HivePerformanceStatistics {

	private static final String READCOUNT = "ReadCount";

	private static final String READFAILURES = "ReadFailures";

	private static final String READTIME = "ReadTime";

	private static final String WRITECOUNT = "WriteCount";

	private static final String WRITEFAILURES = "WriteFailures";

	private static final String WRITETIME = "WriteTime";

	private RollingAverageStatistics stats;

	public HivePerformanceStatisticsMBean(long window, long interval)
			throws NotCompliantMBeanException {
		super(HivePerformanceStatistics.class);

		stats = new RollingAverageStatisticsImpl(Arrays.asList(new String[] {
				READCOUNT, READFAILURES, READTIME, WRITECOUNT,
				WRITEFAILURES, WRITETIME }), window, interval);
	}

	public void addToReadCount(long value) {
		stats.add(READCOUNT, value);
	}

	public void decrementReadCount() {
		stats.decrement(READCOUNT);
	}

	public long getAverageReadCount() {
		return stats.getAverage(READCOUNT);
	}

	public long getIntervalReadCount() {
		return stats.getInterval(READCOUNT);
	}

	public long getMaxReadCount() {
		return stats.getMax(READCOUNT);
	}

	public long getMinReadCount() {
		return stats.getMin(READCOUNT);
	}

	public double getVarianceReadCount() {
		return stats.getVariance(READCOUNT);
	}

	public long getWindowReadCount() {
		return stats.getWindow(READCOUNT);
	}

	public void incrementReadCount() {
		stats.increment(READCOUNT);
	}

	public void addToReadFailures(long value) {
		stats.add(READFAILURES, value);
	}

	public void decrementReadFailures() {
		stats.decrement(READFAILURES);
	}

	public long getAverageReadFailures() {
		return stats.getAverage(READFAILURES);
	}

	public long getIntervalReadFailures() {
		return stats.getInterval(READFAILURES);
	}

	public long getMaxReadFailures() {
		return stats.getMax(READFAILURES);
	}

	public long getMinReadFailures() {
		return stats.getMin(READFAILURES);
	}

	public double getVarianceReadFailures() {
		return stats.getVariance(READFAILURES);
	}

	public long getWindowReadFailures() {
		return stats.getWindow(READFAILURES);
	}

	public void incrementReadFailures() {
		stats.increment(READFAILURES);
	}

	public void addToReadTime(long value) {
		stats.add(READTIME, value);
	}

	public void decrementReadTime() {
		stats.decrement(READTIME);
	}

	public long getAverageReadTime() {
		return stats.getAverage(READTIME);
	}

	public long getIntervalReadTime() {
		return stats.getInterval(READTIME);
	}

	public long getMaxReadTime() {
		return stats.getMax(READTIME);
	}

	public long getMinReadTime() {
		return stats.getMin(READTIME);
	}

	public double getVarianceReadTime() {
		return stats.getVariance(READTIME);
	}

	public long getWindowReadTime() {
		return stats.getWindow(READTIME);
	}

	public void incrementReadTime() {
		stats.increment(READTIME);
	}

	public void addToWriteCount(long value) {
		stats.add(WRITECOUNT, value);
	}

	public void decrementWriteCount() {
		stats.decrement(WRITECOUNT);
	}

	public long getAverageWriteCount() {
		return stats.getAverage(WRITECOUNT);
	}

	public long getIntervalWriteCount() {
		return stats.getInterval(WRITECOUNT);
	}

	public long getMaxWriteCount() {
		return stats.getMax(WRITECOUNT);
	}

	public long getMinWriteCount() {
		return stats.getMin(WRITECOUNT);
	}

	public double getVarianceWriteCount() {
		return stats.getVariance(WRITECOUNT);
	}

	public long getWindowWriteCount() {
		return stats.getWindow(WRITECOUNT);
	}

	public void incrementWriteCount() {
		stats.increment(WRITECOUNT);
	}

	public void addToWriteFailures(long value) {
		stats.add(WRITEFAILURES, value);
	}

	public void decrementWriteFailures() {
		stats.decrement(WRITEFAILURES);
	}

	public long getAverageWriteFailures() {
		return stats.getAverage(WRITEFAILURES);
	}

	public long getIntervalWriteFailures() {
		return stats.getInterval(WRITEFAILURES);
	}

	public long getMaxWriteFailures() {
		return stats.getMax(WRITEFAILURES);
	}

	public long getMinWriteFailures() {
		return stats.getMin(WRITEFAILURES);
	}

	public double getVarianceWriteFailures() {
		return stats.getVariance(WRITEFAILURES);
	}

	public long getWindowWriteFailures() {
		return stats.getWindow(WRITEFAILURES);
	}

	public void incrementWriteFailures() {
		stats.increment(WRITEFAILURES);
	}

	public void addToWriteTime(long value) {
		stats.add(WRITETIME, value);
	}

	public void decrementWriteTime() {
		stats.decrement(WRITETIME);
	}

	public long getAverageWriteTime() {
		return stats.getAverage(WRITETIME);
	}

	public long getIntervalWriteTime() {
		return stats.getInterval(WRITETIME);
	}

	public long getMaxWriteTime() {
		return stats.getMax(WRITETIME);
	}

	public long getMinWriteTime() {
		return stats.getMin(WRITETIME);
	}

	public double getVarianceWriteTime() {
		return stats.getVariance(WRITETIME);
	}

	public long getWindowWriteTime() {
		return stats.getWindow(WRITETIME);
	}

	public void incrementWriteTime() {
		stats.increment(WRITETIME);
	}

}