package org.hivedb.management;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.NodeStatisticsBean;
import org.hivedb.management.statistics.PartitionKeyStatistics;

public class MovePlanValidator {
	private MigrationEstimator estimator;
	
	public MovePlanValidator(MigrationEstimator estimator) {
		this.estimator = estimator;
	}
	
	public SortedSet<NodeStatistics> computeResultingState(SortedSet<NodeStatistics> state, SortedSet<Migration> moves) {
		Map<String, NodeStatistics> stateMap = new Hashtable<String, NodeStatistics>();
		for(NodeStatistics nodeStats : state)
			stateMap.put(nodeStats.getNode().getUri(), nodeStats);
		
		for(Migration move: moves) {
			NodeStatistics origin = stateMap.get(move.getOriginUri());
			NodeStatistics destination = stateMap.get(move.getDestinationUri());
			PartitionKeyStatistics stats = origin.removeParititonKey(move.getPrimaryIndexKey());
			destination.addPartitionKeyStatistics(stats);
			stateMap.put(move.getOriginUri(), origin);
			stateMap.put(move.getDestinationUri(), destination);
		}
		
		SortedSet<NodeStatistics> resultingState = new TreeSet<NodeStatistics>();
		resultingState.addAll(stateMap.values());
		
		return resultingState;
	}


	public boolean isBalanced(Collection<NodeStatistics> state) {
		boolean balanced = true;
		for(NodeStatistics nodeStat : state)
			balanced &= estimator.howMuchDoINeedToMove(nodeStat) == 0;
		return balanced;
	}

	public boolean isValid(SortedSet<NodeStatistics> state, SortedSet<Migration> moves) {
		return isBalanced(computeResultingState(cloneNodeStatisticsList(state), moves));
	}
	
	public static SortedSet<NodeStatistics> cloneNodeStatisticsList(SortedSet<NodeStatistics> original) {
		SortedSet<NodeStatistics> copy = new TreeSet<NodeStatistics>();
		for(NodeStatistics obj : original)
			copy.add(((NodeStatisticsBean)obj).clone());
		return copy;
	}
}