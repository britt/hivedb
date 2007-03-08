package org.hivedb.management.quartz;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hivedb.management.Migration;
import org.hivedb.management.MovePlanValidator;
import org.hivedb.management.statistics.NodeStatistics;
import org.hivedb.management.statistics.PartitionKeyStatistics;
import org.hivedb.util.TestObjectFactory;
import org.testng.annotations.Test;

public class TestMovePlanValidator {
	private static final double NODE_CAPACITY = 100.0;
	@Test
	public void testIsBalanced() {
		MovePlanValidator validator = new MovePlanValidator(TestObjectFactory.halfFullEstimator());
		NodeStatistics firstQuarter = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		NodeStatistics secondQuarter = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		NodeStatistics full = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		firstQuarter.addPartitionKeyStatistics(TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/4));
		secondQuarter.addPartitionKeyStatistics(TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/4));
		full.addPartitionKeyStatistics(TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY));
		
		SortedSet<NodeStatistics> balanced = new TreeSet<NodeStatistics>();
		balanced.add(firstQuarter);
		balanced.add(secondQuarter);
	
		SortedSet<NodeStatistics> unbalanced = new TreeSet<NodeStatistics>();
		unbalanced.add(firstQuarter);
		unbalanced.add(full);
	
		assertTrue(validator.isBalanced(balanced));
		assertFalse(validator.isBalanced(unbalanced));
	}
	
	@Test
	public void testComputeResultingState() {
		MovePlanValidator validator = new MovePlanValidator(TestObjectFactory.halfFullEstimator());
		
		NodeStatistics empty = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		NodeStatistics full = TestObjectFactory.filledNodeStatistics(NODE_CAPACITY, new ArrayList<PartitionKeyStatistics>());
		
		PartitionKeyStatistics firstHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		firstHalf.setKey(new Integer(7));
		PartitionKeyStatistics secondHalf = TestObjectFactory.partitionKeyStats((int)NODE_CAPACITY/2);
		secondHalf.setKey(new Integer(12));
		
		full.addPartitionKeyStatistics(firstHalf);
		full.addPartitionKeyStatistics(secondHalf);
		
		SortedSet<NodeStatistics> startingState = new TreeSet<NodeStatistics>();
		startingState.add(full);
		startingState.add(empty);
		
		SortedSet<Migration> movePlan = new TreeSet<Migration>();
		Migration move = new Migration(firstHalf.getKey(), "dimension", full.getNode().getUri(), empty.getNode().getUri() ,"");
		movePlan.add(move);
		SortedSet<NodeStatistics> resultingState = validator.computeResultingState(startingState, movePlan);
		assertTrue(validator.isBalanced(resultingState));
	}
}
