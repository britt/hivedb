package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.NodeGroup;
import org.hivedb.meta.persistence.NodeGroupDao;
import org.hivedb.util.database.HiveTestCase;
import org.testng.annotations.Test;

public class TestNodeGroupPersistence extends HiveTestCase {
	@Test
	public void testCreate() throws Exception {
		NodeGroupDao dao = new NodeGroupDao(ds);
		assertEquals(0, dao.loadAll().size());
		final NodeGroup group = createEmptyNodeGroup();
		group.setPartitionDimension(createPopulatedPartitionDimension());
		group.getPartitionDimension().updateId(12345);
		dao.create(group);
		assertEquals(1, dao.loadAll().size());
	}
}
