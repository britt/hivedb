package org.hivedb.meta;

import org.hivedb.BucketAssigner;
import org.hivedb.util.database.HiveDbDialect;
import org.junit.Test;import static org.junit.Assert.assertNotNull;import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

public class TestBucketAssigner {

	@Test
	public void testAssignment() {
		Assigner assigner = new BucketAssigner(24);
		Collection<Node> nodes = createNodes(6);
		Node assigned = assigner.chooseNode(nodes, 6);
		assertNotNull(assigned);
		assertEquals(new Integer(0), assigned.getId());
		assigned = assigner.chooseNode(nodes, 1);
		assertNotNull(assigned);
		assertEquals(new Integer(1), assigned.getId());
		assigned = assigner.chooseNode(nodes, 8);
		assertNotNull(assigned);
		assertEquals(new Integer(2), assigned.getId());
	}
	
	private Collection<Node> createNodes(int nodeCount) {
		Collection<Node> nodes = new ArrayList<Node>();
		for(int i=0; i<nodeCount; i++)
			nodes.add(new Node(i,"Node"+i, "NodeUri"+i, "", HiveDbDialect.H2));
		return nodes;
	}
}
