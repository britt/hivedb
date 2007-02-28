package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;

import org.hivedb.util.TestObjectFactory;
import org.testng.annotations.Test;

public class TestNodeStatistics {
	private double NODE_CAPACITY = 100.0;
	
	@Test
	public void fillLevelTest(){
		NodeStatistics node = new NodeStatistics(TestObjectFactory.node(), new ArrayList<PartitionKeyStatistics>(), TestObjectFactory.halfFullEstimator());
		node.getNode().setCapacity(NODE_CAPACITY);
		node.addPartitionKey(TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY));
		assertEquals(NODE_CAPACITY, node.getFillLevel());
	}
	
	@Test
	public void removeKeyTest() {
		NodeStatistics node = new NodeStatistics(TestObjectFactory.node(), new ArrayList<PartitionKeyStatistics>(), TestObjectFactory.halfFullEstimator());
		node.getNode().setCapacity(NODE_CAPACITY);
		PartitionKeyStatistics key = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY);
		node.addPartitionKey(key);
		assertEquals(1, node.getStats().size());
		assertEquals(NODE_CAPACITY, node.getFillLevel());
		node.removeParititonKey(key.getKey());
		assertEquals(0, node.getStats().size());
		assertEquals(0.0, node.getFillLevel());
	}
}
