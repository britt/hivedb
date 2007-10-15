package org.hivedb.meta;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;

import org.testng.annotations.Test;

public class TestBucketAssigner {

	@Test
	public void testAssignment() {
		Assigner assigner = new BucketAssigner(24);
		Collection<Node> nodes = createNodes(6);
		Node assigned = assigner.chooseNode(nodes, 6);
		assertNotNull(assigned);
		assertEquals(0, assigned.getId());
		assigned = assigner.chooseNode(nodes, 1);
		assertNotNull(assigned);
		assertEquals(1, assigned.getId());
		assigned = assigner.chooseNode(nodes, 8);
		assertNotNull(assigned);
		assertEquals(2, assigned.getId());
	}
	
	private Collection<Node> createNodes(int nodeCount) {
		Collection<Node> nodes = new ArrayList<Node>();
		for(int i=0; i<nodeCount; i++)
			nodes.add(new Node(i,"Node"+i, "NodeUri"+i, false, 0));
		return nodes;
	}
}
