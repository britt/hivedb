package org.hivedb.management.migration;

import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.PartitionKeyStatistics;

/**
 * A naive MigrationEstimator that uses user configurable fudge factors
 * to estimate how much to move and how long moves will take.
 * @author bcrawford
 *
 */
public class ConfigurableEstimator implements MigrationEstimator {
	private double maximumSafeFillLevel = 0.75;
	private double secondaryIndexEntriesPerRecord = 1.0;
	private double averageRecordSize = 1.0;
	private double averageMoveTimePerRecord = 1.0;
	
	public static ConfigurableEstimator getInstance() {
		// TODO Make this load configuration from somewhere
		return new ConfigurableEstimator(0.75, 1.0, 1.0, 1.0);
	}
	
	public ConfigurableEstimator(
			double safeFillLevel, 
			double entriesPerRecord, 
			double averageRecordSize, 
			double averageMoveTime) {
		this.maximumSafeFillLevel = safeFillLevel;
		this.secondaryIndexEntriesPerRecord = entriesPerRecord;
		this.averageMoveTimePerRecord = averageMoveTime;
		this.averageRecordSize = averageRecordSize;
	}
	
	public long estimateMoveTime(PartitionKeyStatistics keyStats) {
		return Math.round(estimateSize(keyStats) * averageMoveTimePerRecord);
	}

	public double estimateSize(PartitionKeyStatistics keyStats) {
		return (double)keyStats.getChildRecordCount() * secondaryIndexEntriesPerRecord * averageRecordSize ;
	}

	public double howMuchDoINeedToMove(NodeStatistics stats) {
		return stats.getFillLevel() - (maximumSafeFillLevel * stats.getCapacity());
	}

	public double getAverageMoveTimePerRecord() {
		return averageMoveTimePerRecord;
	}

	public void setAverageMoveTimePerRecord(double averageMoveTimePerRecord) {
		this.averageMoveTimePerRecord = averageMoveTimePerRecord;
	}

	public double getAverageRecordSize() {
		return averageRecordSize;
	}

	public void setAverageRecordSize(double averageRecordSize) {
		this.averageRecordSize = averageRecordSize;
	}

	public double getMaximumSafeFillLevel() {
		return maximumSafeFillLevel;
	}

	public void setMaximumSafeFillLevel(double maximumSafeFillLevel) {
		this.maximumSafeFillLevel = maximumSafeFillLevel;
	}

	public double getSecondaryIndexEntriesPerRecord() {
		return secondaryIndexEntriesPerRecord;
	}

	public void setSecondaryIndexEntriesPerRecord(
			double secondaryIndexEntriesPerRecord) {
		this.secondaryIndexEntriesPerRecord = secondaryIndexEntriesPerRecord;
	}

}
