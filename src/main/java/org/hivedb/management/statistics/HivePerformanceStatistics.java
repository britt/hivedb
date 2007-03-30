package org.hivedb.management.statistics;

public interface HivePerformanceStatistics {
	public long getSumNewReadConnections();
	public long getNewReadConnectionsPerSecond();
	public long getWindowNewReadConnections();
	public long getIntervalNewReadConnections();
	public void addToNewReadConnections(long value);
	public void incrementNewReadConnections();
	
	public long getSumNewWriteConnections();
	public long getNewWriteConnectionsPerSecond();
	public long getWindowNewWriteConnections();
	public long getIntervalNewWriteConnections();
	public void addToNewWriteConnections(long value);
	public void incrementNewWriteConnections();
	
	public long getSumConnectionFailures();
	public long getConnectionFailuresPerSecond();
	public long getWindowConnectionFailures();
	public long getIntervalConnectionFailures();
	public void addToConnectionFailures(long value);
	public void incrementConnectionFailures();
}