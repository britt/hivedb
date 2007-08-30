package org.hivedb.management.statistics;

public interface HivePerformanceStatistics {
	public long getSumNewReadConnections();
	public long getNewReadConnectionsPerSecond();
	public void addToNewReadConnections(long value);
	public void incrementNewReadConnections();
	
	public long getSumNewWriteConnections();
	public long getNewWriteConnectionsPerSecond();
	public void addToNewWriteConnections(long value);
	public void incrementNewWriteConnections();
	
	public long getSumConnectionFailures();
	public long getConnectionFailuresPerSecond();
	public void addToConnectionFailures(long value);
	public void incrementConnectionFailures();
}