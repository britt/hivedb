package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.NodeDao;
import org.hivedb.util.database.DaoTestCase;
import org.testng.annotations.Test;

public class TestNodePersistence extends DaoTestCase {	  
	@Test
	public void testCreate() throws Exception {
		NodeDao dao = new NodeDao(ds);
		assertEquals(0,dao.loadAll().size());
		final Node node = createNode();
		node.setNodeGroup(createEmptyNodeGroup());
		node.getNodeGroup().updateId(12345);
		dao.create(node);
		assertEquals(1,dao.loadAll().size());		
	}
}
