package org.hivedb.management.migration;


import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.PartitionKeyStatistics;

public interface MigrationEstimator {
	public double howMuchDoINeedToMove(NodeStatistics stats);

	public double estimateSize(PartitionKeyStatistics keyStats);
	
	public long estimateMoveTime(PartitionKeyStatistics keyStats);
}
