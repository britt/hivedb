package org.hivedb.management.statistics;

public interface NodeHardwareStatistics {
	
	public long getAverageCpuUtilization();
	public long getMinCpuUtilization();
	public long getMaxCpuUtilization();
	public double getVarianceCpuUtilization();
	public long getWindowCpuUtilization();
	public long getIntervalCpuUtilization();
	public void addToCpuUtilization(long value);
	public void incrementCpuUtilization();
	public void decrementCpuUtilization();
	
	public long getAverageMemoryUtilization();
	public long getMinMemoryUtilization();
	public long getMaxMemoryUtilization();
	public double getVarianceMemoryUtilization();
	public long getWindowMemoryUtilization();
	public long getIntervalMemoryUtilization();
	public void addToMemoryUtilization(long value);
	public void incrementMemoryUtilization();
	public void decrementMemoryUtilization();
	
	public long getAverageDiskIORate();
	public long getMinDiskIORate();
	public long getMaxDiskIORate();
	public double getVarianceDiskIORate();
	public long getWindowDiskIORate();
	public long getIntervalDiskIORate();
	public void addToDiskIORate(long value);
	public void incrementDiskIORate();
	public void decrementDiskIORate();
	
}