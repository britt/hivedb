package org.hivedb.management.statistics;

public interface RollingAverageStatistics {
	public long get(String key);
	public long getWindow(String key);
	public long getInterval(String key);
	public double getVariance(String key);
	public long getMin(String key);
	public long getMax(String key);
	public void add(String key, long value);
	public void increment(String key);
	public void decrement(String key);
}
