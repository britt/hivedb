package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.util.database.H2HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.testng.annotations.Test;

public class TestNodePersistence extends H2HiveTestCase {	  
	@Test
	public void testCreate() throws Exception {
		NodeDao dao = new NodeDao(getDataSource(getHiveDatabaseName()));
		assertEquals(0,dao.loadAll().size());
		final Node node = createNode(getHiveDatabaseName());
		node.setPartitionDimension(createEmptyPartitionDimension());
		dao.create(node);
		List<Node> nodes = dao.loadAll();
		assertEquals(1,nodes.size());		
		assertEquals(node.getName(), Atom.getFirstOrNull(nodes).getName());
		assertEquals(node.getUri(), Atom.getFirstOrNull(nodes).getUri());
	}
}
