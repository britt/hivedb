package org.hivedb.management.statistics;

public interface DirectoryPerformanceStatistics {
	
	public long getAveragePrimaryIndexReadCount();
	public long getMinPrimaryIndexReadCount();
	public long getMaxPrimaryIndexReadCount();
	public double getVariancePrimaryIndexReadCount();
	public long getWindowPrimaryIndexReadCount();
	public long getIntervalPrimaryIndexReadCount();
	public void addToPrimaryIndexReadCount(long value);
	public void incrementPrimaryIndexReadCount();
	public void decrementPrimaryIndexReadCount();
	
	public long getAveragePrimaryIndexReadFailures();
	public long getMinPrimaryIndexReadFailures();
	public long getMaxPrimaryIndexReadFailures();
	public double getVariancePrimaryIndexReadFailures();
	public long getWindowPrimaryIndexReadFailures();
	public long getIntervalPrimaryIndexReadFailures();
	public void addToPrimaryIndexReadFailures(long value);
	public void incrementPrimaryIndexReadFailures();
	public void decrementPrimaryIndexReadFailures();
	
	public long getAveragePrimaryIndexReadTime();
	public long getMinPrimaryIndexReadTime();
	public long getMaxPrimaryIndexReadTime();
	public double getVariancePrimaryIndexReadTime();
	public long getWindowPrimaryIndexReadTime();
	public long getIntervalPrimaryIndexReadTime();
	public void addToPrimaryIndexReadTime(long value);
	public void incrementPrimaryIndexReadTime();
	public void decrementPrimaryIndexReadTime();
	
	public long getAveragePrimaryIndexWriteCount();
	public long getMinPrimaryIndexWriteCount();
	public long getMaxPrimaryIndexWriteCount();
	public double getVariancePrimaryIndexWriteCount();
	public long getWindowPrimaryIndexWriteCount();
	public long getIntervalPrimaryIndexWriteCount();
	public void addToPrimaryIndexWriteCount(long value);
	public void incrementPrimaryIndexWriteCount();
	public void decrementPrimaryIndexWriteCount();
	
	public long getAveragePrimaryIndexWriteFailures();
	public long getMinPrimaryIndexWriteFailures();
	public long getMaxPrimaryIndexWriteFailures();
	public double getVariancePrimaryIndexWriteFailures();
	public long getWindowPrimaryIndexWriteFailures();
	public long getIntervalPrimaryIndexWriteFailures();
	public void addToPrimaryIndexWriteFailures(long value);
	public void incrementPrimaryIndexWriteFailures();
	public void decrementPrimaryIndexWriteFailures();
	
	public long getAveragePrimaryIndexWriteTime();
	public long getMinPrimaryIndexWriteTime();
	public long getMaxPrimaryIndexWriteTime();
	public double getVariancePrimaryIndexWriteTime();
	public long getWindowPrimaryIndexWriteTime();
	public long getIntervalPrimaryIndexWriteTime();
	public void addToPrimaryIndexWriteTime(long value);
	public void incrementPrimaryIndexWriteTime();
	public void decrementPrimaryIndexWriteTime();
	
	public long getAverageSecondaryIndexReadCount();
	public long getMinSecondaryIndexReadCount();
	public long getMaxSecondaryIndexReadCount();
	public double getVarianceSecondaryIndexReadCount();
	public long getWindowSecondaryIndexReadCount();
	public long getIntervalSecondaryIndexReadCount();
	public void addToSecondaryIndexReadCount(long value);
	public void incrementSecondaryIndexReadCount();
	public void decrementSecondaryIndexReadCount();
	
	public long getAverageSecondaryIndexReadFailures();
	public long getMinSecondaryIndexReadFailures();
	public long getMaxSecondaryIndexReadFailures();
	public double getVarianceSecondaryIndexReadFailures();
	public long getWindowSecondaryIndexReadFailures();
	public long getIntervalSecondaryIndexReadFailures();
	public void addToSecondaryIndexReadFailures(long value);
	public void incrementSecondaryIndexReadFailures();
	public void decrementSecondaryIndexReadFailures();
	
	public long getAverageSecondaryIndexReadTime();
	public long getMinSecondaryIndexReadTime();
	public long getMaxSecondaryIndexReadTime();
	public double getVarianceSecondaryIndexReadTime();
	public long getWindowSecondaryIndexReadTime();
	public long getIntervalSecondaryIndexReadTime();
	public void addToSecondaryIndexReadTime(long value);
	public void incrementSecondaryIndexReadTime();
	public void decrementSecondaryIndexReadTime();
	
	public long getAverageSecondaryIndexWriteCount();
	public long getMinSecondaryIndexWriteCount();
	public long getMaxSecondaryIndexWriteCount();
	public double getVarianceSecondaryIndexWriteCount();
	public long getWindowSecondaryIndexWriteCount();
	public long getIntervalSecondaryIndexWriteCount();
	public void addToSecondaryIndexWriteCount(long value);
	public void incrementSecondaryIndexWriteCount();
	public void decrementSecondaryIndexWriteCount();
	
	public long getAverageSecondaryIndexWriteFailures();
	public long getMinSecondaryIndexWriteFailures();
	public long getMaxSecondaryIndexWriteFailures();
	public double getVarianceSecondaryIndexWriteFailures();
	public long getWindowSecondaryIndexWriteFailures();
	public long getIntervalSecondaryIndexWriteFailures();
	public void addToSecondaryIndexWriteFailures(long value);
	public void incrementSecondaryIndexWriteFailures();
	public void decrementSecondaryIndexWriteFailures();
	
	public long getAverageSecondaryIndexWriteTime();
	public long getMinSecondaryIndexWriteTime();
	public long getMaxSecondaryIndexWriteTime();
	public double getVarianceSecondaryIndexWriteTime();
	public long getWindowSecondaryIndexWriteTime();
	public long getIntervalSecondaryIndexWriteTime();
	public void addToSecondaryIndexWriteTime(long value);
	public void incrementSecondaryIndexWriteTime();
	public void decrementSecondaryIndexWriteTime();
	
	public long getAveragePrimaryIndexDeleteCount();
	public long getMinPrimaryIndexDeleteCount();
	public long getMaxPrimaryIndexDeleteCount();
	public double getVariancePrimaryIndexDeleteCount();
	public long getWindowPrimaryIndexDeleteCount();
	public long getIntervalPrimaryIndexDeleteCount();
	public void addToPrimaryIndexDeleteCount(long value);
	public void incrementPrimaryIndexDeleteCount();
	public void decrementPrimaryIndexDeleteCount();
	
	public long getAveragePrimaryIndexDeleteFailures();
	public long getMinPrimaryIndexDeleteFailures();
	public long getMaxPrimaryIndexDeleteFailures();
	public double getVariancePrimaryIndexDeleteFailures();
	public long getWindowPrimaryIndexDeleteFailures();
	public long getIntervalPrimaryIndexDeleteFailures();
	public void addToPrimaryIndexDeleteFailures(long value);
	public void incrementPrimaryIndexDeleteFailures();
	public void decrementPrimaryIndexDeleteFailures();
	
	public long getAveragePrimaryIndexDeleteTime();
	public long getMinPrimaryIndexDeleteTime();
	public long getMaxPrimaryIndexDeleteTime();
	public double getVariancePrimaryIndexDeleteTime();
	public long getWindowPrimaryIndexDeleteTime();
	public long getIntervalPrimaryIndexDeleteTime();
	public void addToPrimaryIndexDeleteTime(long value);
	public void incrementPrimaryIndexDeleteTime();
	public void decrementPrimaryIndexDeleteTime();
	
	public long getAverageSecondaryIndexDeleteCount();
	public long getMinSecondaryIndexDeleteCount();
	public long getMaxSecondaryIndexDeleteCount();
	public double getVarianceSecondaryIndexDeleteCount();
	public long getWindowSecondaryIndexDeleteCount();
	public long getIntervalSecondaryIndexDeleteCount();
	public void addToSecondaryIndexDeleteCount(long value);
	public void incrementSecondaryIndexDeleteCount();
	public void decrementSecondaryIndexDeleteCount();
	
	public long getAverageSecondaryIndexDeleteFailures();
	public long getMinSecondaryIndexDeleteFailures();
	public long getMaxSecondaryIndexDeleteFailures();
	public double getVarianceSecondaryIndexDeleteFailures();
	public long getWindowSecondaryIndexDeleteFailures();
	public long getIntervalSecondaryIndexDeleteFailures();
	public void addToSecondaryIndexDeleteFailures(long value);
	public void incrementSecondaryIndexDeleteFailures();
	public void decrementSecondaryIndexDeleteFailures();
	
	public long getAverageSecondaryIndexDeleteTime();
	public long getMinSecondaryIndexDeleteTime();
	public long getMaxSecondaryIndexDeleteTime();
	public double getVarianceSecondaryIndexDeleteTime();
	public long getWindowSecondaryIndexDeleteTime();
	public long getIntervalSecondaryIndexDeleteTime();
	public void addToSecondaryIndexDeleteTime(long value);
	public void incrementSecondaryIndexDeleteTime();
	public void decrementSecondaryIndexDeleteTime();
	
}