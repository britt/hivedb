package org.hivedb.management.statistics;

import java.util.ArrayList;
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
		long max = Long.MIN_VALUE;
		for(RollingAverage.ObservationInterval observation : this.stats.get(key).getIntervalData())
			max = Math.max( max, divideAsDoubles(observation.getSum(), observation.getCount()));
		return max;
	}

	public long getMin(String key) {
		long min = Long.MAX_VALUE;
		for(RollingAverage.ObservationInterval observation : this.stats.get(key).getIntervalData())
			min = Math.min( min, divideAsDoubles(observation.getSum(), observation.getCount()));
		return min;
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
}
