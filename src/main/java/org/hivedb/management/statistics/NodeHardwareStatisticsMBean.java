package org.hivedb.management.statistics;

import java.util.Arrays;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class NodeHardwareStatisticsMBean extends StandardMBean implements
		NodeHardwareStatistics {

	private static final String CPUUTILIZATION = "CpuUtilization";

	private static final String MEMORYUTILIZATION = "MemoryUtilization";

	private static final String DISKIORATE = "DiskIORate";

	private RollingAverageStatistics stats;

	public NodeHardwareStatisticsMBean(long window, long interval)
			throws NotCompliantMBeanException {
		super(NodeHardwareStatistics.class);

		stats = new RollingAverageStatisticsImpl(Arrays.asList(new String[] {
				CPUUTILIZATION, MEMORYUTILIZATION, DISKIORATE }), window,
				interval);
	}

	public void addToCpuUtilization(long value) {
		stats.add(CPUUTILIZATION, value);
	}

	public void decrementCpuUtilization() {
		stats.decrement(CPUUTILIZATION);
	}

	public long getAverageCpuUtilization() {
		return stats.getAverage(CPUUTILIZATION);
	}

	public long getIntervalCpuUtilization() {
		return stats.getInterval(CPUUTILIZATION);
	}

	public long getMaxCpuUtilization() {
		return stats.getMax(CPUUTILIZATION);
	}

	public long getMinCpuUtilization() {
		return stats.getMin(CPUUTILIZATION);
	}

	public double getVarianceCpuUtilization() {
		return stats.getVariance(CPUUTILIZATION);
	}

	public long getWindowCpuUtilization() {
		return stats.getWindow(CPUUTILIZATION);
	}

	public void incrementCpuUtilization() {
		stats.increment(CPUUTILIZATION);
	}

	public void addToMemoryUtilization(long value) {
		stats.add(MEMORYUTILIZATION, value);
	}

	public void decrementMemoryUtilization() {
		stats.decrement(MEMORYUTILIZATION);
	}

	public long getAverageMemoryUtilization() {
		return stats.getAverage(MEMORYUTILIZATION);
	}

	public long getIntervalMemoryUtilization() {
		return stats.getInterval(MEMORYUTILIZATION);
	}

	public long getMaxMemoryUtilization() {
		return stats.getMax(MEMORYUTILIZATION);
	}

	public long getMinMemoryUtilization() {
		return stats.getMin(MEMORYUTILIZATION);
	}

	public double getVarianceMemoryUtilization() {
		return stats.getVariance(MEMORYUTILIZATION);
	}

	public long getWindowMemoryUtilization() {
		return stats.getWindow(MEMORYUTILIZATION);
	}

	public void incrementMemoryUtilization() {
		stats.increment(MEMORYUTILIZATION);
	}

	public void addToDiskIORate(long value) {
		stats.add(DISKIORATE, value);
	}

	public void decrementDiskIORate() {
		stats.decrement(DISKIORATE);
	}

	public long getAverageDiskIORate() {
		return stats.getAverage(DISKIORATE);
	}

	public long getIntervalDiskIORate() {
		return stats.getInterval(DISKIORATE);
	}

	public long getMaxDiskIORate() {
		return stats.getMax(DISKIORATE);
	}

	public long getMinDiskIORate() {
		return stats.getMin(DISKIORATE);
	}

	public double getVarianceDiskIORate() {
		return stats.getVariance(DISKIORATE);
	}

	public long getWindowDiskIORate() {
		return stats.getWindow(DISKIORATE);
	}

	public void incrementDiskIORate() {
		stats.increment(DISKIORATE);
	}

}