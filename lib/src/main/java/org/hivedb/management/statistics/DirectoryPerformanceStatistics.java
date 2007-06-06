package org.hivedb.management.statistics;

public interface DirectoryPerformanceStatistics {
	
	public long getAveragePrimaryIndexReadCount();
	public long getPrimaryIndexReadCount();
	public long getMinPrimaryIndexReadCount();
	public long getMaxPrimaryIndexReadCount();
	public void addToPrimaryIndexReadCount(long value);
	public void incrementPrimaryIndexReadCount();
	public void decrementPrimaryIndexReadCount();
	
	public long getAveragePrimaryIndexReadFailures();
	public long getPrimaryIndexReadFailures();
	public long getMinPrimaryIndexReadFailures();
	public long getMaxPrimaryIndexReadFailures();
	public void addToPrimaryIndexReadFailures(long value);
	public void incrementPrimaryIndexReadFailures();
	public void decrementPrimaryIndexReadFailures();
	
	public long getAveragePrimaryIndexReadTime();
	public long getMinPrimaryIndexReadTime();
	public long getMaxPrimaryIndexReadTime();
	public void addToPrimaryIndexReadTime(long value);
	public void incrementPrimaryIndexReadTime();
	public void decrementPrimaryIndexReadTime();
	
	public long getAveragePrimaryIndexWriteCount();
	public long getPrimaryIndexWriteCount();
	public long getMinPrimaryIndexWriteCount();
	public long getMaxPrimaryIndexWriteCount();
	public void addToPrimaryIndexWriteCount(long value);
	public void incrementPrimaryIndexWriteCount();
	public void decrementPrimaryIndexWriteCount();
	
	public long getAveragePrimaryIndexWriteFailures();
	public long getPrimaryIndexWriteFailures();
	public long getMinPrimaryIndexWriteFailures();
	public long getMaxPrimaryIndexWriteFailures();
	public void addToPrimaryIndexWriteFailures(long value);
	public void incrementPrimaryIndexWriteFailures();
	public void decrementPrimaryIndexWriteFailures();
	
	public long getAveragePrimaryIndexWriteTime();
	public long getMinPrimaryIndexWriteTime();
	public long getMaxPrimaryIndexWriteTime();
	public void addToPrimaryIndexWriteTime(long value);
	public void incrementPrimaryIndexWriteTime();
	public void decrementPrimaryIndexWriteTime();
	
	public long getAverageSecondaryIndexReadCount();
	public long getSecondaryIndexReadCount();
	public long getMinSecondaryIndexReadCount();
	public long getMaxSecondaryIndexReadCount();
	public void addToSecondaryIndexReadCount(long value);
	public void incrementSecondaryIndexReadCount();
	public void decrementSecondaryIndexReadCount();
	
	public long getAverageSecondaryIndexReadFailures();
	public long getSecondaryIndexReadFailures();
	public long getMinSecondaryIndexReadFailures();
	public long getMaxSecondaryIndexReadFailures();
	public void addToSecondaryIndexReadFailures(long value);
	public void incrementSecondaryIndexReadFailures();
	public void decrementSecondaryIndexReadFailures();
	
	public long getAverageSecondaryIndexReadTime();
	public long getMinSecondaryIndexReadTime();
	public long getMaxSecondaryIndexReadTime();
	public void addToSecondaryIndexReadTime(long value);
	public void incrementSecondaryIndexReadTime();
	public void decrementSecondaryIndexReadTime();
	
	public long getAverageSecondaryIndexWriteCount();
	public long getSecondaryIndexWriteCount();
	public long getMinSecondaryIndexWriteCount();
	public long getMaxSecondaryIndexWriteCount();
	public void addToSecondaryIndexWriteCount(long value);
	public void incrementSecondaryIndexWriteCount();
	public void decrementSecondaryIndexWriteCount();
	
	public long getAverageSecondaryIndexWriteFailures();
	public long getSecondaryIndexWriteFailures();
	public long getMinSecondaryIndexWriteFailures();
	public long getMaxSecondaryIndexWriteFailures();
	public void addToSecondaryIndexWriteFailures(long value);
	public void incrementSecondaryIndexWriteFailures();
	public void decrementSecondaryIndexWriteFailures();
	
	public long getAverageSecondaryIndexWriteTime();
	public long getMinSecondaryIndexWriteTime();
	public long getMaxSecondaryIndexWriteTime();
	public void addToSecondaryIndexWriteTime(long value);
	public void incrementSecondaryIndexWriteTime();
	public void decrementSecondaryIndexWriteTime();
	
	public long getAveragePrimaryIndexDeleteCount();
	public long getPrimaryIndexDeleteCount();
	public long getMinPrimaryIndexDeleteCount();
	public long getMaxPrimaryIndexDeleteCount();
	public void addToPrimaryIndexDeleteCount(long value);
	public void incrementPrimaryIndexDeleteCount();
	public void decrementPrimaryIndexDeleteCount();
	
	public long getAveragePrimaryIndexDeleteFailures();
	public long getPrimaryIndexDeleteFailures();
	public long getMinPrimaryIndexDeleteFailures();
	public long getMaxPrimaryIndexDeleteFailures();
	public void addToPrimaryIndexDeleteFailures(long value);
	public void incrementPrimaryIndexDeleteFailures();
	public void decrementPrimaryIndexDeleteFailures();
	
	public long getAveragePrimaryIndexDeleteTime();
	public long getMinPrimaryIndexDeleteTime();
	public long getMaxPrimaryIndexDeleteTime();
	public void addToPrimaryIndexDeleteTime(long value);
	public void incrementPrimaryIndexDeleteTime();
	public void decrementPrimaryIndexDeleteTime();
	
	public long getAverageSecondaryIndexDeleteCount();
	public long getSecondaryIndexDeleteCount();
	public long getMinSecondaryIndexDeleteCount();
	public long getMaxSecondaryIndexDeleteCount();
	public void addToSecondaryIndexDeleteCount(long value);
	public void incrementSecondaryIndexDeleteCount();
	public void decrementSecondaryIndexDeleteCount();
	
	public long getAverageSecondaryIndexDeleteFailures();
	public long getSecondaryIndexDeleteFailures();
	public long getMinSecondaryIndexDeleteFailures();
	public long getMaxSecondaryIndexDeleteFailures();
	public void addToSecondaryIndexDeleteFailures(long value);
	public void incrementSecondaryIndexDeleteFailures();
	public void decrementSecondaryIndexDeleteFailures();
	
	public long getAverageSecondaryIndexDeleteTime();
	public long getMinSecondaryIndexDeleteTime();
	public long getMaxSecondaryIndexDeleteTime();
	public void addToSecondaryIndexDeleteTime(long value);
	public void incrementSecondaryIndexDeleteTime();
	public void decrementSecondaryIndexDeleteTime();
	
}