package org.hivedb.management.statistics;

import java.util.Arrays;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class NodeFillStatisticsMBean extends StandardMBean implements
		NodeFillStatistics {
	private static final String FILLLEVEL = "FillLevel";
	private static final String FILLRATE = "FillRate";

	private long capacity = 0;
	private RollingAverageStatistics stats;
	
	public NodeFillStatisticsMBean(long capacity, long window, long interval)
			throws NotCompliantMBeanException {
		super(NodeFillStatistics.class);
		this.capacity = capacity;
		stats = new RollingAverageStatisticsImpl(Arrays.asList(new String[] {
				FILLLEVEL, FILLRATE }), window, interval);
	}

	public void addToFillLevel(long value) {
		stats.add(FILLLEVEL, value);
	}

	public void decrementFillLevel() {
		stats.decrement(FILLLEVEL);
	}

	public long getAverageFillLevel() {
		return stats.getAverage(FILLLEVEL);
	}

	public long getIntervalFillLevel() {
		return stats.getInterval(FILLLEVEL);
	}

	public long getMaxFillLevel() {
		return stats.getMax(FILLLEVEL);
	}

	public long getMinFillLevel() {
		return stats.getMin(FILLLEVEL);
	}

	public double getVarianceFillLevel() {
		return stats.getVariance(FILLLEVEL);
	}

	public long getWindowFillLevel() {
		return stats.getWindow(FILLLEVEL);
	}

	public void incrementFillLevel() {
		stats.increment(FILLLEVEL);
	}

	public void addToFillRate(long value) {
		stats.add(FILLRATE, value);
	}

	public void decrementFillRate() {
		stats.decrement(FILLRATE);
	}

	public long getAverageFillRate() {
		return stats.getAverage(FILLRATE);
	}

	public long getIntervalFillRate() {
		return stats.getInterval(FILLRATE);
	}

	public long getMaxFillRate() {
		return stats.getMax(FILLRATE);
	}

	public long getMinFillRate() {
		return stats.getMin(FILLRATE);
	}

	public double getVarianceFillRate() {
		return stats.getVariance(FILLRATE);
	}

	public long getWindowFillRate() {
		return stats.getWindow(FILLRATE);
	}

	public void incrementFillRate() {
		stats.increment(FILLRATE);
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}
}