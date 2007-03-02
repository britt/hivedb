package org.hivedb.management;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.hivedb.management.quartz.Migration;
import org.hivedb.management.quartz.NodeBalancer;
import org.hivedb.management.quartz.NodeBalancerImpl;
import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.management.statistics.PartitionKeyStatisticsBean;
import org.hivedb.meta.Assigner;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.Hive;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.Node;
import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.TestObjectFactory;
import org.testng.annotations.Test;

public class NodeBalancerTest extends DerbyTestCase {
	private static final double NODE_CAPACITY = 4.0;
	
	@Test
	public void keySuggestionTest() {
		NodeBalancerImpl balancer = 
			new NodeBalancerImpl(TestObjectFactory.partitionDimension(), TestObjectFactory.halfFullEstimator(), null);
		
		NodeStatistics full = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		
		PartitionKeyStatisticsBean firstHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		firstHalf.setKey(new Integer(7));
		PartitionKeyStatisticsBean secondHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		secondHalf.setKey(new Integer(12));
		
		full.addPartitionKey(firstHalf);
		full.addPartitionKey(secondHalf);
		
		SortedSet<PartitionKeyStatistics> keysToMove = balancer.suggestKeysToMove(full);
	
		assertEquals(1, keysToMove.size());
		assertEquals((int)NODE_CAPACITY/2, keysToMove.first().getChildRecordCount());
	}
	
	@Test
	public void suggestDestinationTest() {
		NodeBalancerImpl balancer = 
			new NodeBalancerImpl(TestObjectFactory.partitionDimension(), TestObjectFactory.halfFullEstimator(), null);
		
		NodeStatistics empty = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		NodeStatistics full = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		
		PartitionKeyStatisticsBean firstHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		firstHalf.setKey(new Integer(7));
		firstHalf.setChildRecordCount((int)NODE_CAPACITY/2);
		PartitionKeyStatisticsBean secondHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		secondHalf.setKey(new Integer(12));
		secondHalf.setChildRecordCount((int)NODE_CAPACITY/2);
		
		full.addPartitionKey(firstHalf);
		full.addPartitionKey(secondHalf);
		
		SortedSet<PartitionKeyStatistics> keysToMove = balancer.suggestKeysToMove(full);
	
		SortedSet<NodeStatistics> nodes = new TreeSet<NodeStatistics>();
		nodes.add(full);
		nodes.add(empty);
		
		SortedSet<Migration> moves = balancer.pairMigrantsWithDestinations(full.getNode(), keysToMove, nodes);
		assertEquals(1, moves.size());
		assertEquals(full.getNode(), moves.first().getOrigin());
		assertEquals(empty.getNode(), moves.first().getDestination());
	}
	
	@Test
	public void testSuggestionCorrectness() {
		NodeBalancerImpl balancer = 
			new NodeBalancerImpl(TestObjectFactory.partitionDimension(), TestObjectFactory.halfFullEstimator(), null);
		
		NodeStatistics full = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		
		PartitionKeyStatisticsBean firstQuarter = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/4);
		firstQuarter.setKey(new Integer(7));
		PartitionKeyStatisticsBean secondQuarter = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/4);
		secondQuarter.setKey(new Integer(9));
		PartitionKeyStatisticsBean lastHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		lastHalf.setKey(new Integer(12));
		
		full.addPartitionKey(firstQuarter);
		full.addPartitionKey(secondQuarter);
		full.addPartitionKey(lastHalf);
		
		SortedSet<PartitionKeyStatistics> keysToMove = balancer.suggestKeysToMove(full);
	
		assertEquals(2, keysToMove.size());
		for(PartitionKeyStatistics key : keysToMove)
			assertEquals((int)NODE_CAPACITY/4, key.getChildRecordCount());
	}
	
	@Test
	public void moveSuggestionTest() throws Exception {
		Collection<Node> nodes = new ArrayList<Node>();
		nodes.add(TestObjectFactory.node());
		nodes.add(TestObjectFactory.node());
		
		Collection<Resource> resources = new ArrayList<Resource>();
		resources.add(TestObjectFactory.resource());
		NodeGroup group = new NodeGroup(nodes);
		PartitionDimension dimension = TestObjectFactory.partitionDimension(Types.INTEGER, group, getConnectString(), resources);
		dimension.setAssigner(new RoundRobinAssigner());
		
		Hive hive = initializeHive(nodes, resources, dimension);
		
		hive.getPartitionDimension("aDimension").setAssigner(new RoundRobinAssigner());
		NodeBalancer balancer = 
			new NodeBalancerImpl(
					hive.getPartitionDimension("aDimension"), 
					TestObjectFactory.halfFullEstimator(), 
					new HiveBasicDataSource(getConnectString()));
		
		SecondaryIndex secondaryIndex = TestObjectFactory.secondaryIndex("werd");
		secondaryIndex.setResource(resources.iterator().next());
		
		int count=0;
		Integer fullKey1 = new Integer(7);
		Integer fullKey2 = new Integer(9);
		Integer emptyKey = new Integer(12);
		//counting on the custom assigner and the fact that there are only 2 nodes here.
		hive.insertPrimaryIndexKey("aDimension", fullKey1);
		hive.insertPrimaryIndexKey("aDimension", emptyKey);
		hive.insertPrimaryIndexKey("aDimension", fullKey2);
		
		for(int i=0; i<NODE_CAPACITY/2; i++){
			hive.insertSecondaryIndexKey(secondaryIndex, new Integer(++count), fullKey1);
			hive.insertSecondaryIndexKey(secondaryIndex, new Integer(++count), fullKey2);
		}
		
		Node full = hive.getNodeOfPrimaryIndexKey(dimension, fullKey1);
		Node empty = hive.getNodeOfPrimaryIndexKey(dimension, emptyKey);
		
		SortedSet<Migration> moves = balancer.suggestMoves(nodes);
		
		assertEquals(1, moves.size());
		assertEquals(full, moves.first().getOrigin());
		assertEquals(empty, moves.first().getDestination());
	}
	
	public Hive initializeHive(Collection<Node> nodes, Collection<Resource> resources, PartitionDimension dimension) {
		DataSource ds = new HiveBasicDataSource(getConnectString());
		IndexSchema schema = new IndexSchema(dimension);
		Hive hive = null;
		try {
			new GlobalSchema(getConnectString()).install();
			new HiveSemaphoreDao(ds).create();
			schema.install();
			hive = Hive.load(getConnectString());
			hive.addPartitionDimension(dimension);
			SecondaryIndex secondaryIndex = TestObjectFactory.secondaryIndex("werd");

			for(Resource resource : resources)
				hive.addSecondaryIndex(resource, secondaryIndex);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception while installing schema.");
		}
		return hive;
	}
	private static int assignments = 1;
	class RoundRobinAssigner implements Assigner {
		
		public Node chooseNode(Collection<Node> nodes, Object value) {
			ArrayList<Node> n = new ArrayList<Node>(nodes);
			return n.get(++assignments % n.size());
		}		
	}
}
