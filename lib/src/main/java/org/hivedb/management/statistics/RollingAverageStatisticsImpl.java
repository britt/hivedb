package org.hivedb.management.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.hivedb.util.RollingAverage;
import org.hivedb.util.functional.DebugMap;

public class RollingAverageStatisticsImpl implements RollingAverageStatistics {
	private Map<String,RollingAverage> stats;
	private long step = 1;
	
	private RollingAverageStatisticsImpl() {
		this.stats = new DebugMap<String, RollingAverage>();
	}
	
	public RollingAverageStatisticsImpl(Collection<String> keys, long windowSize, long intervalSize) {
		this();
		for(String key : keys)
			this.registerCounter(key, windowSize, intervalSize);
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

	public long getAverage(String key) {
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

	public long getMax(String key) {
		return this.stats.get(key).getMax();
	}

	public long getMin(String key) {
		return this.stats.get(key).getMin();
	}

	public double getVariance(String key) {
		Collection<RollingAverage.ObservationInterval> observations = this.stats.get(key).getIntervalData();
		long sum = 0;
		for(RollingAverage.ObservationInterval observation : observations)
			sum += Math.pow(divideAsDoubles(observation.getSum(), observation.getCount()) - this.getAverage(key), 2);
		return (double) sum / observations.size();
	}
	
	private long divideAsDoubles(long numerator, long denominator) {
		return Math.round( (double) numerator / denominator);
	}

	public Collection<String> listStatistics() {
		Collection<String> keys = new ArrayList<String>();
		for(String key : this.stats.keySet())
			keys.add(key);
		return keys;
	}

	public void registerCounter(String name, long window, long interval) {
		this.stats.put(name, RollingAverage.getInstanceByIntervalSize(window, interval));
	}

	public long getSum(String key) {
		return this.stats.get(key).getSum();
	}

	public long getTimeAverage(String key, long periodInMillis) {
		return Math.round( (double) stats.get(key).getSum() / ((double) stats.get(key).getWindowSize() / periodInMillis) );
	}
}
