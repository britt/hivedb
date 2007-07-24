package org.hivedb.util;

import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.hivedb.management.migration.MigrationEstimator;
import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.NodeStatisticsBean;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsBean;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;

public class TestObjectFactory {
	public static NodeStatistics filledNodeStatistics(double capacity, List<PartitionKeyStatistics> stats) {
		NodeStatistics s = new NodeStatisticsBean(node(), stats, halfFullEstimator());
		s.getNode().setCapacity(capacity);
		return s;
	}
	
	public static Node node() {
		return node(0.0);
	}
	
	public static Node node(double capacity) {
		Node node = new Node("aNode" + new Random().nextInt(),"aNode" + new Random().nextInt(), false);
		node.setCapacity(capacity);
		return node;
	}
	
	public static PartitionKeyStatisticsBean partitionKeyStats(int fill){
		PartitionKeyStatisticsBean key = new PartitionKeyStatisticsBean(null, new Random().nextInt(), new Date(System.currentTimeMillis()));
		key.setChildRecordCount(fill);
		return key;
	}
	
	public static PartitionDimension partitionDimension() {
		return new PartitionDimension("aDimension", Types.INTEGER);
	}
	
	public static PartitionDimension partitionDimension(int columnType, Collection<Node> nodes, String uri, Collection<Resource> resources) {
		return new PartitionDimension(new Random().nextInt(), "aDimension", columnType, nodes, uri, resources);
	}
	
	public static MigrationEstimator halfFullEstimator() {
		return new MigrationEstimator() {

			public long estimateMoveTime(PartitionKeyStatistics keyStats) {
				return 0;
			}

			public double estimateSize(PartitionKeyStatistics keyStats) {
				return keyStats.getChildRecordCount();
			}

			public double howMuchDoINeedToMove(NodeStatistics stats) {
				return stats.getFillLevel() > stats.getCapacity()/2.0 ? 
						stats.getFillLevel() - stats.getCapacity()/2.0 
						: 0.0;
			}
		};
	}
	
	public static Resource resource() {
		return new Resource(new Random().nextInt(), "aResource", new ArrayList<SecondaryIndex>());
	}
	
	public static SecondaryIndex secondaryIndex(String name) {
		return new SecondaryIndex(name, Types.INTEGER);
	}
}
