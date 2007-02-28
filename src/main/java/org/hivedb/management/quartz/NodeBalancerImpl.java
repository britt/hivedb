package org.hivedb.management.quartz;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;

// TODO Extract interfaces (?)
// TODO I don't like the Impl naming
// TODO Add move plan validation
// TODO Determine what to do when  a valid plan cannot be found

public class NodeBalancerImpl implements NodeBalancer {
	private PartitionDimension dimension;
	private DataSource ds;
	private SortedSet<NodeStatistics> startingState;
	private MigrationEstimator estimator;
	
	public NodeBalancerImpl(PartitionDimension dimension, MigrationEstimator estimator, DataSource ds) {
		this.dimension = dimension;
		this.estimator = estimator;
		this.ds = ds;
	}
	
	public SortedSet<Migration> suggestMoves(Collection<Node> nodes) {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(ds);
		
		//Build a list of node statistics sorted by fill level (descending).
		startingState = new TreeSet<NodeStatistics>();
		for(Node node : nodes) {
			List<PartitionKeyStatistics> stats = dao.findAllByNodeAndDimension(dimension, node);
			startingState.add(new NodeStatistics(node, stats, estimator));
		}
		
		//Compute the moves needed to balance each node
		SortedSet<Migration> moves = new TreeSet<Migration>();
		for(NodeStatistics stats : startingState){
			SortedSet<NodeStatistics> validDestinations = cloneNodeStatsList(startingState);
			validDestinations.remove(stats);
			moves.addAll( pairMigrantsWithDestinations(stats.getNode(), suggestKeysToMove(stats), validDestinations));
		}
		return moves;
	}

	private SortedSet<NodeStatistics> cloneNodeStatsList(SortedSet<NodeStatistics> original) {
		SortedSet<NodeStatistics> copy = new TreeSet<NodeStatistics>();
		for(NodeStatistics obj : original)
			copy.add(obj.clone());
		return copy;
	}
	
	public SortedSet<PartitionKeyStatistics> suggestKeysToMove(NodeStatistics nodeStats) {
		SortedSet<PartitionKeyStatistics> keyStats = new TreeSet<PartitionKeyStatistics>();
		keyStats.addAll(nodeStats.getStats());
		
		double spaceToBeFreed = estimator.howMuchDoINeedToMove(nodeStats);
		SortedSet<PartitionKeyStatistics> keysToMove = new TreeSet<PartitionKeyStatistics>();
		
		for(int i=0; i< keyStats.size() && spaceFreedByMoves(keysToMove) < spaceToBeFreed; i++){
			keysToMove.add(keyStats.first());
			keyStats.remove(keyStats.first());
		}
			
		return keysToMove;
	}
	
	public SortedSet<Migration> pairMigrantsWithDestinations(Node origin, SortedSet<PartitionKeyStatistics> migrants, SortedSet<NodeStatistics> destinations) {
		SortedSet<Migration> movePlan = new TreeSet<Migration>();
		
		for(PartitionKeyStatistics migrant : migrants) {
			NodeStatistics destination = destinations.first();
			destinations.first().addPartitionKey(migrant);
			movePlan.add(new Migration(migrant.getKey(), origin, destination.getNode()));
		}
		return movePlan;
	}
	
	public double spaceFreedByMoves(SortedSet<PartitionKeyStatistics> moves) {
		double sum = 0;
		for(PartitionKeyStatistics move: moves)
			sum += estimator.estimateSize(move);
		return sum;
	}

}
