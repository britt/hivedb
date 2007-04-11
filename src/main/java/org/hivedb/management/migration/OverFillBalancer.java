package org.hivedb.management.migration;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.NodeStatisticsBean;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsDao;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.HiveBasicDataSource;

// TODO Implement Partial Move plans
// TODO Do more than just punt when a plan cannot be found

public class OverFillBalancer implements NodeBalancer {
	private PartitionDimension dimension;
	private String uri;
	private SortedSet<NodeStatistics> startingState;
	private MigrationEstimator estimator;
	
	public OverFillBalancer(PartitionDimension dimension, MigrationEstimator estimator, String uri) {
		this.dimension = dimension;
		this.estimator = estimator;
		this.uri = uri;
	}
	
	public SortedSet<Migration> suggestMoves(Collection<Node> nodes) throws MigrationPlanningException {
		PartitionKeyStatisticsDao dao = new PartitionKeyStatisticsDao(new HiveBasicDataSource(uri));
		
		//Build a list of node statistics sorted by fill level (descending).
		startingState = new TreeSet<NodeStatistics>();
		for(Node node : nodes) {
			List<PartitionKeyStatistics> stats = dao.findAllByNodeAndDimension(dimension, node);
			startingState.add(new NodeStatisticsBean(node, stats, estimator));
		}
		
		//Compute the moves needed to balance each node
		SortedSet<Migration> moves = new TreeSet<Migration>();
		for(NodeStatistics stats : startingState){
			SortedSet<NodeStatistics> validDestinations = MovePlanValidator.cloneNodeStatisticsList(startingState);
			validDestinations.remove(stats);
			moves.addAll( pairMigrantsWithDestinations(stats.getNode(), suggestKeysToMove(stats), validDestinations));
		}
		
		MovePlanValidator validator = new MovePlanValidator(estimator);
		if(validator.isValid(startingState, moves))
			return moves;
		else
			throw new MigrationPlanningException();
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
	
	public SortedSet<Migration> pairMigrantsWithDestinations(Node origin, SortedSet<PartitionKeyStatistics> migrants, SortedSet<NodeStatistics> destinations) throws MigrationPlanningException {
		SortedSet<Migration> movePlan = new TreeSet<Migration>();
		for(PartitionKeyStatistics migrant : migrants) {
			NodeStatistics destination = destinations.first();
			destination.addPartitionKeyStatistics(migrant);
			movePlan.add(new Migration(migrant.getKey(), migrant.getPartitionDimension().getName(), origin.getUri(), destination.getNode().getUri(), uri));
			if(estimator.howMuchDoINeedToMove(destination) > 0)
				throw new MigrationPlanningException();
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
