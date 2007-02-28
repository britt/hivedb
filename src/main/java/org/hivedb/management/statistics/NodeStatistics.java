/**
 * 
 */
package org.hivedb.management.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.hivedb.management.quartz.MigrationEstimator;
import org.hivedb.meta.Node;
import org.hivedb.util.HiveUtils;

public class NodeStatistics implements Comparable, Cloneable{
	private Node node;
	private MigrationEstimator estimator;
	private List<PartitionKeyStatistics> stats;
	
	private NodeStatistics() {
		this.stats = new ArrayList<PartitionKeyStatistics>();
	}
	
	public NodeStatistics( 
			Node node, 
			List<PartitionKeyStatistics> stats, 
			MigrationEstimator estimator) {
		this();
		this.node = node;
		this.stats.addAll(stats);
		this.estimator = estimator;
	}

	public Node getNode() {
		return node;
	}

	public double getFillLevel() {
		double used = 0.0;
		for(PartitionKeyStatistics keyStats : getStats()) 
			used += estimator.estimateSize(keyStats);		
		return used;
	}

	public List<PartitionKeyStatistics> getStats() {
		return stats;
	}

	public void addPartitionKey(PartitionKeyStatistics keyStats) {
		this.stats.add(keyStats);
	}
	
	public PartitionKeyStatistics removeParititonKey(Object key) {
		for(PartitionKeyStatistics keyStats: stats) {
			if(keyStats.getKey().equals(key))
				return stats.remove(stats.indexOf(keyStats));
		}
		throw new NoSuchElementException();
	}
	
	public double getCapacity() {
		return node.getCapacity();
	}

	public boolean equals(Object obj)
	{
		return obj.hashCode() == hashCode();
	}
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[] {node, stats, estimator});
	}

	public int compareTo(Object o) {
		NodeStatistics s = (NodeStatistics)o;
		if(this.equals(s))
			return 0;
		else if(this.getFillLevel() > s.getFillLevel())
			return 1;
		else if(this.getFillLevel() < s.getFillLevel())
			return -1;
		else
			return new Integer(this.hashCode()).compareTo(s.hashCode());
	}
	
	public NodeStatistics clone() {
		NodeStatistics clone = new NodeStatistics(getNode(), new ArrayList<PartitionKeyStatistics>(),estimator);
		for(PartitionKeyStatistics s : getStats())
			clone.addPartitionKey(s.clone());
		return clone;
	}
}