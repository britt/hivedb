package org.hivedb.management.statistics;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import org.hivedb.util.RollingAverage;

public class RollingAverageStatisticsImpl implements RollingAverageStatistics {
	private Map<String,RollingAverage> stats;
	private long step = 1;
	
	private RollingAverageStatisticsImpl() {
		this.stats = new Hashtable<String, RollingAverage>();
	}
	
	public RollingAverageStatisticsImpl(Collection<String> keys, long windowSize, long intervalSize) {
		this();
		for(String key : keys)
			this.stats.put(key, RollingAverage.getInstanceByIntervalSize(windowSize, intervalSize));
	}
	
	public void add(String key, long value) {
		this.stats.get(key).add(value);
	}

	public void increment(String key) {
		this.stats.get(key).add(step);
	}
	
	public void decrement(String key) {
		this.stats.get(key).add(-1*step);
	}

	public long get(String key) {
		return this.stats.get(key).getAverage();
	}

	public long getInterval(String key) {
		return this.stats.get(key).getIntervalSize();
	}

	public long getWindow(String key) {
		return this.stats.get(key).getWindowSize();
	}

	public long getStep() {
		return step;
	}

	public void setStep(long step) {
		this.step = step;
	}

}
