/**
 * 
 */
package org.hivedb.management.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.hivedb.management.MigrationEstimator;
import org.hivedb.meta.Node;
import org.hivedb.util.HiveUtils;

public class NodeStatisticsBean implements Comparable, Cloneable, NodeStatistics{
	private Node node;
	private MigrationEstimator estimator;
	private List<PartitionKeyStatistics> stats;
	
	private NodeStatisticsBean() {
		this.stats = new ArrayList<PartitionKeyStatistics>();
	}
	
	public NodeStatisticsBean( 
			Node node, 
			List<PartitionKeyStatistics> stats, 
			MigrationEstimator estimator) {
		this();
		this.node = node;
		this.stats.addAll(stats);
		this.estimator = estimator;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#getNode()
	 */
	public Node getNode() {
		return node;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#getFillLevel()
	 */
	public double getFillLevel() {
		double used = 0.0;
		for(PartitionKeyStatistics keyStats : getStats()) 
			used += estimator.estimateSize(keyStats);		
		return used;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#getStats()
	 */
	public List<PartitionKeyStatistics> getStats() {
		return stats;
	}

	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#addPartitionKey(org.hivedb.management.statistics.PartitionKeyStatistics)
	 */
	public void addPartitionKey(PartitionKeyStatistics keyStats) {
		this.stats.add(keyStats);
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#removeParititonKey(java.lang.Object)
	 */
	public PartitionKeyStatistics removeParititonKey(Object key) {
		for(PartitionKeyStatistics keyStats: stats) {
			if(keyStats.getKey().equals(key))
				return stats.remove(stats.indexOf(keyStats));
		}
		throw new NoSuchElementException();
	}
	
	/* (non-Javadoc)
	 * @see org.hivedb.management.statistics.INodeStats#getCapacity()
	 */
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
		NodeStatistics clone = new NodeStatisticsBean(getNode(), new ArrayList<PartitionKeyStatistics>(),estimator);
		for(PartitionKeyStatistics s : getStats())
			clone.addPartitionKey(((PartitionKeyStatisticsBean) s).clone());
		return clone;
	}
}