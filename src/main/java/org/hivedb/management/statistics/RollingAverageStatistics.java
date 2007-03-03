package org.hivedb.management.statistics;

import java.util.Collection;

/**
 * An interface for statistics collected using a rolling average.
 * @author bcrawford
 *
 */
public interface RollingAverageStatistics {
	
	/**
	 * @return a list of the keys for statistics that maybe requested.
	 */
	public Collection<String> listStatistics();
	
	/**
	 * @param key The name of statistic being fetched.
	 * @return the average of the specified statistic for the most recent time window.
	 */
	public long getAverage(String key);
	
	/**
	 * @param key The name of statistic being fetched.
	 * @return the length of the averaging window in millis.
	 */
	public long getWindow(String key);
	
	/**
	 * @param key The name of statistic being fetched.
	 * @return the duration of the sampling interval with the window in millis.
	 */
	public long getInterval(String key);
	
	/**
	 * @param key The name of statistic being fetched.
	 * @return the variance (standard deviation squared) of the sampling intervals
	 */
	public double getVariance(String key);
	
	/**
	 * Get the minimum value of the sampling interval within the current window.
	 * @param key The name of statistic being fetched.
	 * @return
	 */
	public long getMin(String key);
	
	/**
	 * Get the maximum value of the sampling interval within the current window.
	 * @param key The name of statistic being fetched.
	 * @return
	 */
	public long getMax(String key);
	
	/**
	 * Add the value to the current sample.
	 * @param key The name of statistic being fetched.
	 * @param value 
	 */
	public void add(String key, long value);
	
	/**
	 * Add one to the current sample.
	 * @param key The name of statistic being fetched.
	 */
	public void increment(String key);
	
	/**
	 * Subtract one from the current sample.
	 * @param key The name of statistic being fetched.
	 */
	public void decrement(String key);
	
	/**
	 * Add a new statistics to be measured
	 * @param name
	 * @param window
	 * @param interval
	 */
	public void registerCounter(String name, long window, long interval);
}
