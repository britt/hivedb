package org.hivedb.management.statistics;

public interface Counter {
	public void add(String key, long value);
	public void dercrement(String key);
	public void increment(String key);

}