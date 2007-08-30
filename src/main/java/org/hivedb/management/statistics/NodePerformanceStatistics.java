package org.hivedb.management.statistics;

public interface NodePerformanceStatistics {
	
	public long getAverageReadCount();
	public long getMinReadCount();
	public long getMaxReadCount();
	public void addToReadCount(long value);
	public void incrementReadCount();
	public void decrementReadCount();
	
	public long getAverageReadFailures();
	public long getMinReadFailures();
	public long getMaxReadFailures();
	public void addToReadFailures(long value);
	public void incrementReadFailures();
	public void decrementReadFailures();

	public long getAverageWriteCount();
	public long getMinWriteCount();
	public long getMaxWriteCount();
	public void addToWriteCount(long value);
	public void incrementWriteCount();
	public void decrementWriteCount();
	
	public long getAverageWriteFailures();
	public long getMinWriteFailures();
	public long getMaxWriteFailures();
	public void addToWriteFailures(long value);
	public void incrementWriteFailures();
	public void decrementWriteFailures();
}