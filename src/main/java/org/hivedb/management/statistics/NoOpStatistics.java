/**
 * 
 */
package org.hivedb.management.statistics;


public class NoOpStatistics implements Counter {

	public void add(String key, long value) {}

	public void decrement(String key) {}

	public void increment(String key) {}

}