package org.hivedb.management.statistics;

public interface NodeFillStatistics {
	
	public long getAverageFillLevel();
	public long getMinFillLevel();
	public long getMaxFillLevel();
	public double getVarianceFillLevel();
	public long getWindowFillLevel();
	public long getIntervalFillLevel();
	public void addToFillLevel(long value);
	public void incrementFillLevel();
	public void decrementFillLevel();
	
	public long getAverageFillRate();
	public long getMinFillRate();
	public long getMaxFillRate();
	public double getVarianceFillRate();
	public long getWindowFillRate();
	public long getIntervalFillRate();
	public void addToFillRate(long value);
	public void incrementFillRate();
	public void decrementFillRate();
	
	public long getCapacity();
}