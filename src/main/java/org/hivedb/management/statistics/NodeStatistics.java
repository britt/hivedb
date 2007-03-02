package org.hivedb.management.statistics;

import java.util.List;

import org.hivedb.meta.Node;

public interface NodeStatistics {

	public abstract Node getNode();

	public abstract double getFillLevel();

	public abstract List<PartitionKeyStatistics> getStats();

	public abstract void addPartitionKey(PartitionKeyStatistics keyStats);

	public abstract PartitionKeyStatistics removeParititonKey(Object key);

	public abstract double getCapacity();

}