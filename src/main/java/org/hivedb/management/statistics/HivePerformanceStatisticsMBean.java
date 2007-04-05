package org.hivedb.management.statistics;

import java.util.Arrays;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class HivePerformanceStatisticsMBean extends StandardMBean implements HivePerformanceStatistics, Counter {
	
	private static final String NEWREADCONNECTIONS = "NewReadConnections";
	
	private static final String NEWWRITECONNECTIONS = "NewWriteConnections";
	
	private static final String CONNECTIONFAILURES = "ConnectionFailures";
	
	private RollingAverageStatistics stats;
	
	public HivePerformanceStatisticsMBean(long window, long interval) throws NotCompliantMBeanException{
		super(HivePerformanceStatistics.class);
		
		stats = new RollingAverageStatisticsImpl(Arrays.asList(new String[] {NEWREADCONNECTIONS,NEWWRITECONNECTIONS,CONNECTIONFAILURES}), window, interval);
	}
	
	
	
	public void addToNewReadConnections(long value) {
		stats.add(NEWREADCONNECTIONS, value);
	}
	public long getSumNewReadConnections() {
		return stats.getSum(NEWREADCONNECTIONS);
	}
	public long getIntervalNewReadConnections() {
		return stats.getInterval(NEWREADCONNECTIONS);
	}
	public long getWindowNewReadConnections() {
		return stats.getWindow(NEWREADCONNECTIONS);
	}
	public void incrementNewReadConnections() {
		stats.increment(NEWREADCONNECTIONS);
	}
	
	
	public void addToNewWriteConnections(long value) {
		stats.add(NEWWRITECONNECTIONS, value);
	}
	public long getSumNewWriteConnections() {
		return stats.getSum(NEWWRITECONNECTIONS);
	}
	public long getIntervalNewWriteConnections() {
		return stats.getInterval(NEWWRITECONNECTIONS);
	}
	public long getWindowNewWriteConnections() {
		return stats.getWindow(NEWWRITECONNECTIONS);
	}
	public void incrementNewWriteConnections() {
		stats.increment(NEWWRITECONNECTIONS);
	}
	
	
	public void addToConnectionFailures(long value) {
		stats.add(CONNECTIONFAILURES, value);
	}
	public long getSumConnectionFailures() {
		return stats.getSum(CONNECTIONFAILURES);
	}
	public long getIntervalConnectionFailures() {
		return stats.getInterval(CONNECTIONFAILURES);
	}
	public long getWindowConnectionFailures() {
		return stats.getWindow(CONNECTIONFAILURES);
	}
	public void incrementConnectionFailures() {
		stats.increment(CONNECTIONFAILURES);
	}



	public long getConnectionFailuresPerSecond() {return stats.getTimeAverage(CONNECTIONFAILURES, 1000);}
	public long getNewReadConnectionsPerSecond() {return stats.getTimeAverage(NEWREADCONNECTIONS, 1000);}
	public long getNewWriteConnectionsPerSecond() {return stats.getTimeAverage(NEWWRITECONNECTIONS, 1000);}
	
	
	public void add(String key, long value) {
		stats.add(key, value);
	}
	
	public void dercrement(String key) {
		stats.decrement(key);
	}
	
	public void increment(String key) {
		stats.increment(key);
	}
	
}