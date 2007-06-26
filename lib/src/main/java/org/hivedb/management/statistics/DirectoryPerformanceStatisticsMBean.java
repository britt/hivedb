package org.hivedb.management.statistics;

import java.util.Arrays;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class DirectoryPerformanceStatisticsMBean extends StandardMBean implements DirectoryPerformanceStatistics, Counter {
	
	public static final String PRIMARYINDEXREADCOUNT = "PrimaryIndexReadCount";
	
	public static final String PRIMARYINDEXREADFAILURES = "PrimaryIndexReadFailures";
	
	public static final String PRIMARYINDEXREADTIME = "PrimaryIndexReadTime";
	
	public static final String PRIMARYINDEXWRITECOUNT = "PrimaryIndexWriteCount";
	
	public static final String PRIMARYINDEXWRITEFAILURES = "PrimaryIndexWriteFailures";
	
	public static final String PRIMARYINDEXWRITETIME = "PrimaryIndexWriteTime";
	
	public static final String SECONDARYINDEXREADCOUNT = "SecondaryIndexReadCount";
	
	public static final String SECONDARYINDEXREADFAILURES = "SecondaryIndexReadFailures";
	
	public static final String SECONDARYINDEXREADTIME = "SecondaryIndexReadTime";
	
	public static final String SECONDARYINDEXWRITECOUNT = "SecondaryIndexWriteCount";
	
	public static final String SECONDARYINDEXWRITEFAILURES = "SecondaryIndexWriteFailures";
	
	public static final String SECONDARYINDEXWRITETIME = "SecondaryIndexWriteTime";

	public static final String PRIMARYINDEXDELETECOUNT = "PrimaryIndexDeleteCount";
	
	public static final String PRIMARYINDEXDELETETIME = "PrimaryIndexDeleteTime";
	
	public static final String PRIMARYINDEXDELETEFAILURES = "PrimaryIndexDeleteFailures";

	public static final String SECONDARYINDEXDELETECOUNT = "SecondaryIndexDeletes";

	public static final String SECONDARYINDEXDELETEFAILURES = "SecondaryIndexDeleteFailures";

	public static final String SECONDARYINDEXDELETETIME = "SecondaryIndexDeleteTime";
	
	private RollingAverageStatistics stats;
	
	//Exception required by StandardMBean
	public DirectoryPerformanceStatisticsMBean(long window, long interval) throws NotCompliantMBeanException{
		super(DirectoryPerformanceStatistics.class);
		
		stats = new RollingAverageStatisticsImpl(Arrays.asList(new String[] {
				PRIMARYINDEXREADCOUNT,
				PRIMARYINDEXREADFAILURES,
				PRIMARYINDEXREADTIME,
				PRIMARYINDEXWRITECOUNT,
				PRIMARYINDEXWRITEFAILURES,
				PRIMARYINDEXWRITETIME,
				SECONDARYINDEXREADCOUNT,
				SECONDARYINDEXREADFAILURES,
				SECONDARYINDEXREADTIME,
				SECONDARYINDEXWRITECOUNT,
				SECONDARYINDEXWRITEFAILURES,
				SECONDARYINDEXWRITETIME,
				PRIMARYINDEXDELETECOUNT,	
				PRIMARYINDEXDELETETIME,
				PRIMARYINDEXDELETEFAILURES,
				SECONDARYINDEXDELETECOUNT,
				SECONDARYINDEXDELETEFAILURES,
				SECONDARYINDEXDELETETIME}), window, interval);
	}

	
	
	
	public void addToPrimaryIndexReadCount(long value) {
		stats.add(PRIMARYINDEXREADCOUNT, value);
	}
	public void decrementPrimaryIndexReadCount() {
		stats.decrement(PRIMARYINDEXREADCOUNT);
	}
	public long getAveragePrimaryIndexReadCount() {
		return stats.getAverage(PRIMARYINDEXREADCOUNT);
	}
	public long getIntervalPrimaryIndexReadCount() {
		return stats.getInterval(PRIMARYINDEXREADCOUNT);
	}
	public long getMaxPrimaryIndexReadCount() {
		return stats.getMax(PRIMARYINDEXREADCOUNT);
	}
	public long getMinPrimaryIndexReadCount() {
		return stats.getMin(PRIMARYINDEXREADCOUNT);
	}
	public double getVariancePrimaryIndexReadCount() {
		return stats.getVariance(PRIMARYINDEXREADCOUNT);
	}
	public long getWindowPrimaryIndexReadCount() {
		return stats.getWindow(PRIMARYINDEXREADCOUNT);
	}
	public void incrementPrimaryIndexReadCount() {
		stats.increment(PRIMARYINDEXREADCOUNT);
	}
	
	
	public void addToPrimaryIndexReadFailures(long value) {
		stats.add(PRIMARYINDEXREADFAILURES, value);
	}
	public void decrementPrimaryIndexReadFailures() {
		stats.decrement(PRIMARYINDEXREADFAILURES);
	}
	public long getAveragePrimaryIndexReadFailures() {
		return stats.getAverage(PRIMARYINDEXREADFAILURES);
	}
	public long getIntervalPrimaryIndexReadFailures() {
		return stats.getInterval(PRIMARYINDEXREADFAILURES);
	}
	public long getMaxPrimaryIndexReadFailures() {
		return stats.getMax(PRIMARYINDEXREADFAILURES);
	}
	public long getMinPrimaryIndexReadFailures() {
		return stats.getMin(PRIMARYINDEXREADFAILURES);
	}
	public double getVariancePrimaryIndexReadFailures() {
		return stats.getVariance(PRIMARYINDEXREADFAILURES);
	}
	public long getWindowPrimaryIndexReadFailures() {
		return stats.getWindow(PRIMARYINDEXREADFAILURES);
	}
	public void incrementPrimaryIndexReadFailures() {
		stats.increment(PRIMARYINDEXREADFAILURES);
	}
	
	
	public void addToPrimaryIndexReadTime(long value) {
		stats.add(PRIMARYINDEXREADTIME, value);
	}
	public void decrementPrimaryIndexReadTime() {
		stats.decrement(PRIMARYINDEXREADTIME);
	}
	public long getAveragePrimaryIndexReadTime() {
		return stats.getAverage(PRIMARYINDEXREADTIME);
	}
	public long getIntervalPrimaryIndexReadTime() {
		return stats.getInterval(PRIMARYINDEXREADTIME);
	}
	public long getMaxPrimaryIndexReadTime() {
		return stats.getMax(PRIMARYINDEXREADTIME);
	}
	public long getMinPrimaryIndexReadTime() {
		return stats.getMin(PRIMARYINDEXREADTIME);
	}
	public double getVariancePrimaryIndexReadTime() {
		return stats.getVariance(PRIMARYINDEXREADTIME);
	}
	public long getWindowPrimaryIndexReadTime() {
		return stats.getWindow(PRIMARYINDEXREADTIME);
	}
	public void incrementPrimaryIndexReadTime() {
		stats.increment(PRIMARYINDEXREADTIME);
	}
	
	
	public void addToPrimaryIndexWriteCount(long value) {
		stats.add(PRIMARYINDEXWRITECOUNT, value);
	}
	public void decrementPrimaryIndexWriteCount() {
		stats.decrement(PRIMARYINDEXWRITECOUNT);
	}
	public long getAveragePrimaryIndexWriteCount() {
		return stats.getAverage(PRIMARYINDEXWRITECOUNT);
	}
	public long getIntervalPrimaryIndexWriteCount() {
		return stats.getInterval(PRIMARYINDEXWRITECOUNT);
	}
	public long getMaxPrimaryIndexWriteCount() {
		return stats.getMax(PRIMARYINDEXWRITECOUNT);
	}
	public long getMinPrimaryIndexWriteCount() {
		return stats.getMin(PRIMARYINDEXWRITECOUNT);
	}
	public double getVariancePrimaryIndexWriteCount() {
		return stats.getVariance(PRIMARYINDEXWRITECOUNT);
	}
	public long getWindowPrimaryIndexWriteCount() {
		return stats.getWindow(PRIMARYINDEXWRITECOUNT);
	}
	public void incrementPrimaryIndexWriteCount() {
		stats.increment(PRIMARYINDEXWRITECOUNT);
	}
	
	
	public void addToPrimaryIndexWriteFailures(long value) {
		stats.add(PRIMARYINDEXWRITEFAILURES, value);
	}
	public void decrementPrimaryIndexWriteFailures() {
		stats.decrement(PRIMARYINDEXWRITEFAILURES);
	}
	public long getAveragePrimaryIndexWriteFailures() {
		return stats.getAverage(PRIMARYINDEXWRITEFAILURES);
	}
	public long getIntervalPrimaryIndexWriteFailures() {
		return stats.getInterval(PRIMARYINDEXWRITEFAILURES);
	}
	public long getMaxPrimaryIndexWriteFailures() {
		return stats.getMax(PRIMARYINDEXWRITEFAILURES);
	}
	public long getMinPrimaryIndexWriteFailures() {
		return stats.getMin(PRIMARYINDEXWRITEFAILURES);
	}
	public double getVariancePrimaryIndexWriteFailures() {
		return stats.getVariance(PRIMARYINDEXWRITEFAILURES);
	}
	public long getWindowPrimaryIndexWriteFailures() {
		return stats.getWindow(PRIMARYINDEXWRITEFAILURES);
	}
	public void incrementPrimaryIndexWriteFailures() {
		stats.increment(PRIMARYINDEXWRITEFAILURES);
	}
	
	
	public void addToPrimaryIndexWriteTime(long value) {
		stats.add(PRIMARYINDEXWRITETIME, value);
	}
	public void decrementPrimaryIndexWriteTime() {
		stats.decrement(PRIMARYINDEXWRITETIME);
	}
	public long getAveragePrimaryIndexWriteTime() {
		return stats.getAverage(PRIMARYINDEXWRITETIME);
	}
	public long getIntervalPrimaryIndexWriteTime() {
		return stats.getInterval(PRIMARYINDEXWRITETIME);
	}
	public long getMaxPrimaryIndexWriteTime() {
		return stats.getMax(PRIMARYINDEXWRITETIME);
	}
	public long getMinPrimaryIndexWriteTime() {
		return stats.getMin(PRIMARYINDEXWRITETIME);
	}
	public double getVariancePrimaryIndexWriteTime() {
		return stats.getVariance(PRIMARYINDEXWRITETIME);
	}
	public long getWindowPrimaryIndexWriteTime() {
		return stats.getWindow(PRIMARYINDEXWRITETIME);
	}
	public void incrementPrimaryIndexWriteTime() {
		stats.increment(PRIMARYINDEXWRITETIME);
	}
	
	
	public void addToSecondaryIndexReadCount(long value) {
		stats.add(SECONDARYINDEXREADCOUNT, value);
	}
	public void decrementSecondaryIndexReadCount() {
		stats.decrement(SECONDARYINDEXREADCOUNT);
	}
	public long getAverageSecondaryIndexReadCount() {
		return stats.getAverage(SECONDARYINDEXREADCOUNT);
	}
	public long getIntervalSecondaryIndexReadCount() {
		return stats.getInterval(SECONDARYINDEXREADCOUNT);
	}
	public long getMaxSecondaryIndexReadCount() {
		return stats.getMax(SECONDARYINDEXREADCOUNT);
	}
	public long getMinSecondaryIndexReadCount() {
		return stats.getMin(SECONDARYINDEXREADCOUNT);
	}
	public double getVarianceSecondaryIndexReadCount() {
		return stats.getVariance(SECONDARYINDEXREADCOUNT);
	}
	public long getWindowSecondaryIndexReadCount() {
		return stats.getWindow(SECONDARYINDEXREADCOUNT);
	}
	public void incrementSecondaryIndexReadCount() {
		stats.increment(SECONDARYINDEXREADCOUNT);
	}
	
	
	public void addToSecondaryIndexReadFailures(long value) {
		stats.add(SECONDARYINDEXREADFAILURES, value);
	}
	public void decrementSecondaryIndexReadFailures() {
		stats.decrement(SECONDARYINDEXREADFAILURES);
	}
	public long getAverageSecondaryIndexReadFailures() {
		return stats.getAverage(SECONDARYINDEXREADFAILURES);
	}
	public long getIntervalSecondaryIndexReadFailures() {
		return stats.getInterval(SECONDARYINDEXREADFAILURES);
	}
	public long getMaxSecondaryIndexReadFailures() {
		return stats.getMax(SECONDARYINDEXREADFAILURES);
	}
	public long getMinSecondaryIndexReadFailures() {
		return stats.getMin(SECONDARYINDEXREADFAILURES);
	}
	public double getVarianceSecondaryIndexReadFailures() {
		return stats.getVariance(SECONDARYINDEXREADFAILURES);
	}
	public long getWindowSecondaryIndexReadFailures() {
		return stats.getWindow(SECONDARYINDEXREADFAILURES);
	}
	public void incrementSecondaryIndexReadFailures() {
		stats.increment(SECONDARYINDEXREADFAILURES);
	}
	
	
	public void addToSecondaryIndexReadTime(long value) {
		stats.add(SECONDARYINDEXREADTIME, value);
	}
	public void decrementSecondaryIndexReadTime() {
		stats.decrement(SECONDARYINDEXREADTIME);
	}
	public long getAverageSecondaryIndexReadTime() {
		return stats.getAverage(SECONDARYINDEXREADTIME);
	}
	public long getIntervalSecondaryIndexReadTime() {
		return stats.getInterval(SECONDARYINDEXREADTIME);
	}
	public long getMaxSecondaryIndexReadTime() {
		return stats.getMax(SECONDARYINDEXREADTIME);
	}
	public long getMinSecondaryIndexReadTime() {
		return stats.getMin(SECONDARYINDEXREADTIME);
	}
	public double getVarianceSecondaryIndexReadTime() {
		return stats.getVariance(SECONDARYINDEXREADTIME);
	}
	public long getWindowSecondaryIndexReadTime() {
		return stats.getWindow(SECONDARYINDEXREADTIME);
	}
	public void incrementSecondaryIndexReadTime() {
		stats.increment(SECONDARYINDEXREADTIME);
	}
	
	
	public void addToSecondaryIndexWriteCount(long value) {
		stats.add(SECONDARYINDEXWRITECOUNT, value);
	}
	public void decrementSecondaryIndexWriteCount() {
		stats.decrement(SECONDARYINDEXWRITECOUNT);
	}
	public long getAverageSecondaryIndexWriteCount() {
		return stats.getAverage(SECONDARYINDEXWRITECOUNT);
	}
	public long getIntervalSecondaryIndexWriteCount() {
		return stats.getInterval(SECONDARYINDEXWRITECOUNT);
	}
	public long getMaxSecondaryIndexWriteCount() {
		return stats.getMax(SECONDARYINDEXWRITECOUNT);
	}
	public long getMinSecondaryIndexWriteCount() {
		return stats.getMin(SECONDARYINDEXWRITECOUNT);
	}
	public double getVarianceSecondaryIndexWriteCount() {
		return stats.getVariance(SECONDARYINDEXWRITECOUNT);
	}
	public long getWindowSecondaryIndexWriteCount() {
		return stats.getWindow(SECONDARYINDEXWRITECOUNT);
	}
	public void incrementSecondaryIndexWriteCount() {
		stats.increment(SECONDARYINDEXWRITECOUNT);
	}
	
	
	public void addToSecondaryIndexWriteFailures(long value) {
		stats.add(SECONDARYINDEXWRITEFAILURES, value);
	}
	public void decrementSecondaryIndexWriteFailures() {
		stats.decrement(SECONDARYINDEXWRITEFAILURES);
	}
	public long getAverageSecondaryIndexWriteFailures() {
		return stats.getAverage(SECONDARYINDEXWRITEFAILURES);
	}
	public long getIntervalSecondaryIndexWriteFailures() {
		return stats.getInterval(SECONDARYINDEXWRITEFAILURES);
	}
	public long getMaxSecondaryIndexWriteFailures() {
		return stats.getMax(SECONDARYINDEXWRITEFAILURES);
	}
	public long getMinSecondaryIndexWriteFailures() {
		return stats.getMin(SECONDARYINDEXWRITEFAILURES);
	}
	public double getVarianceSecondaryIndexWriteFailures() {
		return stats.getVariance(SECONDARYINDEXWRITEFAILURES);
	}
	public long getWindowSecondaryIndexWriteFailures() {
		return stats.getWindow(SECONDARYINDEXWRITEFAILURES);
	}
	public void incrementSecondaryIndexWriteFailures() {
		stats.increment(SECONDARYINDEXWRITEFAILURES);
	}
	
	
	public void addToSecondaryIndexWriteTime(long value) {
		stats.add(SECONDARYINDEXWRITETIME, value);
	}
	public void decrementSecondaryIndexWriteTime() {
		stats.decrement(SECONDARYINDEXWRITETIME);
	}
	public long getAverageSecondaryIndexWriteTime() {
		return stats.getAverage(SECONDARYINDEXWRITETIME);
	}
	public long getIntervalSecondaryIndexWriteTime() {
		return stats.getInterval(SECONDARYINDEXWRITETIME);
	}
	public long getMaxSecondaryIndexWriteTime() {
		return stats.getMax(SECONDARYINDEXWRITETIME);
	}
	public long getMinSecondaryIndexWriteTime() {
		return stats.getMin(SECONDARYINDEXWRITETIME);
	}
	public double getVarianceSecondaryIndexWriteTime() {
		return stats.getVariance(SECONDARYINDEXWRITETIME);
	}
	public long getWindowSecondaryIndexWriteTime() {
		return stats.getWindow(SECONDARYINDEXWRITETIME);
	}
	public void incrementSecondaryIndexWriteTime() {
		stats.increment(SECONDARYINDEXWRITETIME);
	}
	
	public void addToPrimaryIndexDeleteCount(long value) {
		stats.add(PRIMARYINDEXDELETECOUNT, value);
	}
	public void decrementPrimaryIndexDeleteCount() {
		stats.decrement(PRIMARYINDEXDELETECOUNT);
	}
	public long getAveragePrimaryIndexDeleteCount() {
		return stats.getAverage(PRIMARYINDEXDELETECOUNT);
	}
	public long getIntervalPrimaryIndexDeleteCount() {
		return stats.getInterval(PRIMARYINDEXDELETECOUNT);
	}
	public long getMaxPrimaryIndexDeleteCount() {
		return stats.getMax(PRIMARYINDEXDELETECOUNT);
	}
	public long getMinPrimaryIndexDeleteCount() {
		return stats.getMin(PRIMARYINDEXDELETECOUNT);
	}
	public double getVariancePrimaryIndexDeleteCount() {
		return stats.getVariance(PRIMARYINDEXDELETECOUNT);
	}
	public long getWindowPrimaryIndexDeleteCount() {
		return stats.getWindow(PRIMARYINDEXDELETECOUNT);
	}
	public void incrementPrimaryIndexDeleteCount() {
		stats.increment(PRIMARYINDEXDELETECOUNT);
	}
	
	
	public void addToPrimaryIndexDeleteFailures(long value) {
		stats.add(PRIMARYINDEXDELETEFAILURES, value);
	}
	public void decrementPrimaryIndexDeleteFailures() {
		stats.decrement(PRIMARYINDEXDELETEFAILURES);
	}
	public long getAveragePrimaryIndexDeleteFailures() {
		return stats.getAverage(PRIMARYINDEXDELETEFAILURES);
	}
	public long getIntervalPrimaryIndexDeleteFailures() {
		return stats.getInterval(PRIMARYINDEXDELETEFAILURES);
	}
	public long getMaxPrimaryIndexDeleteFailures() {
		return stats.getMax(PRIMARYINDEXDELETEFAILURES);
	}
	public long getMinPrimaryIndexDeleteFailures() {
		return stats.getMin(PRIMARYINDEXDELETEFAILURES);
	}
	public double getVariancePrimaryIndexDeleteFailures() {
		return stats.getVariance(PRIMARYINDEXDELETEFAILURES);
	}
	public long getWindowPrimaryIndexDeleteFailures() {
		return stats.getWindow(PRIMARYINDEXDELETEFAILURES);
	}
	public void incrementPrimaryIndexDeleteFailures() {
		stats.increment(PRIMARYINDEXDELETEFAILURES);
	}
	
	
	public void addToPrimaryIndexDeleteTime(long value) {
		stats.add(PRIMARYINDEXDELETETIME, value);
	}
	public void decrementPrimaryIndexDeleteTime() {
		stats.decrement(PRIMARYINDEXDELETETIME);
	}
	public long getAveragePrimaryIndexDeleteTime() {
		return stats.getAverage(PRIMARYINDEXDELETETIME);
	}
	public long getIntervalPrimaryIndexDeleteTime() {
		return stats.getInterval(PRIMARYINDEXDELETETIME);
	}
	public long getMaxPrimaryIndexDeleteTime() {
		return stats.getMax(PRIMARYINDEXDELETETIME);
	}
	public long getMinPrimaryIndexDeleteTime() {
		return stats.getMin(PRIMARYINDEXDELETETIME);
	}
	public double getVariancePrimaryIndexDeleteTime() {
		return stats.getVariance(PRIMARYINDEXDELETETIME);
	}
	public long getWindowPrimaryIndexDeleteTime() {
		return stats.getWindow(PRIMARYINDEXDELETETIME);
	}
	public void incrementPrimaryIndexDeleteTime() {
		stats.increment(PRIMARYINDEXDELETETIME);
	}
	
	public void addToSecondaryIndexDeleteCount(long value) {
		stats.add(SECONDARYINDEXDELETECOUNT, value);
	}
	public void decrementSecondaryIndexDeleteCount() {
		stats.decrement(SECONDARYINDEXDELETECOUNT);
	}
	public long getAverageSecondaryIndexDeleteCount() {
		return stats.getAverage(SECONDARYINDEXDELETECOUNT);
	}
	public long getIntervalSecondaryIndexDeleteCount() {
		return stats.getInterval(SECONDARYINDEXDELETECOUNT);
	}
	public long getMaxSecondaryIndexDeleteCount() {
		return stats.getMax(SECONDARYINDEXDELETECOUNT);
	}
	public long getMinSecondaryIndexDeleteCount() {
		return stats.getMin(SECONDARYINDEXDELETECOUNT);
	}
	public double getVarianceSecondaryIndexDeleteCount() {
		return stats.getVariance(SECONDARYINDEXDELETECOUNT);
	}
	public long getWindowSecondaryIndexDeleteCount() {
		return stats.getWindow(SECONDARYINDEXDELETECOUNT);
	}
	public void incrementSecondaryIndexDeleteCount() {
		stats.increment(SECONDARYINDEXDELETECOUNT);
	}
	
	
	public void addToSecondaryIndexDeleteFailures(long value) {
		stats.add(SECONDARYINDEXDELETEFAILURES, value);
	}
	public void decrementSecondaryIndexDeleteFailures() {
		stats.decrement(SECONDARYINDEXDELETEFAILURES);
	}
	public long getAverageSecondaryIndexDeleteFailures() {
		return stats.getAverage(SECONDARYINDEXDELETEFAILURES);
	}
	public long getIntervalSecondaryIndexDeleteFailures() {
		return stats.getInterval(SECONDARYINDEXDELETEFAILURES);
	}
	public long getMaxSecondaryIndexDeleteFailures() {
		return stats.getMax(SECONDARYINDEXDELETEFAILURES);
	}
	public long getMinSecondaryIndexDeleteFailures() {
		return stats.getMin(SECONDARYINDEXDELETEFAILURES);
	}
	public double getVarianceSecondaryIndexDeleteFailures() {
		return stats.getVariance(SECONDARYINDEXDELETEFAILURES);
	}
	public long getWindowSecondaryIndexDeleteFailures() {
		return stats.getWindow(SECONDARYINDEXDELETEFAILURES);
	}
	public void incrementSecondaryIndexDeleteFailures() {
		stats.increment(SECONDARYINDEXDELETEFAILURES);
	}
	
	
	public void addToSecondaryIndexDeleteTime(long value) {
		stats.add(SECONDARYINDEXDELETETIME, value);
	}
	public void decrementSecondaryIndexDeleteTime() {
		stats.decrement(SECONDARYINDEXDELETETIME);
	}
	public long getAverageSecondaryIndexDeleteTime() {
		return stats.getAverage(SECONDARYINDEXDELETETIME);
	}
	public long getIntervalSecondaryIndexDeleteTime() {
		return stats.getInterval(SECONDARYINDEXDELETETIME);
	}
	public long getMaxSecondaryIndexDeleteTime() {
		return stats.getMax(SECONDARYINDEXDELETETIME);
	}
	public long getMinSecondaryIndexDeleteTime() {
		return stats.getMin(SECONDARYINDEXDELETETIME);
	}
	public double getVarianceSecondaryIndexDeleteTime() {
		return stats.getVariance(SECONDARYINDEXDELETETIME);
	}
	public long getWindowSecondaryIndexDeleteTime() {
		return stats.getWindow(SECONDARYINDEXDELETETIME);
	}
	public void incrementSecondaryIndexDeleteTime() {
		stats.increment(SECONDARYINDEXDELETETIME);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.Counter#add(java.lang.String, long)
	 */
	public void add(String key, long value) {
		stats.add(key, value);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.Counter#dercrement(java.lang.String)
	 */
	public void decrement(String key) {
		stats.decrement(key);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.Counter#increment(java.lang.String)
	 */
	public void increment(String key) {
		stats.increment(key);
	}




	public long getPrimaryIndexDeleteCount() {
		return stats.getSum(PRIMARYINDEXDELETECOUNT);
	}




	public long getPrimaryIndexDeleteFailures() {
		return stats.getSum(PRIMARYINDEXDELETEFAILURES);
	}




	public long getPrimaryIndexReadCount() {
		return stats.getSum(PRIMARYINDEXREADCOUNT);
	}




	public long getPrimaryIndexReadFailures() {
		return stats.getSum(PRIMARYINDEXREADFAILURES);
	}




	public long getPrimaryIndexWriteCount() {
		return stats.getSum(SECONDARYINDEXWRITECOUNT);
	}




	public long getPrimaryIndexWriteFailures() {
		return stats.getSum(SECONDARYINDEXWRITEFAILURES);
	}




	public long getSecondaryIndexDeleteCount() {
		return stats.getSum(SECONDARYINDEXDELETECOUNT);
	}




	public long getSecondaryIndexDeleteFailures() {
		return stats.getSum(SECONDARYINDEXDELETEFAILURES);
	}




	public long getSecondaryIndexReadCount() {
		return stats.getSum(SECONDARYINDEXREADCOUNT);
	}




	public long getSecondaryIndexReadFailures() {
		return stats.getSum(SECONDARYINDEXREADFAILURES);
	}




	public long getSecondaryIndexWriteCount() {
		return stats.getSum(SECONDARYINDEXWRITECOUNT);
	}




	public long getSecondaryIndexWriteFailures() {
		return stats.getSum(SECONDARYINDEXWRITEFAILURES);
	}
}