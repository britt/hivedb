package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.PartitionDimensionDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.Test;

public class TestPartitionDimensionPersistence extends H2HiveTestCase {
	@Test
	public void testCreate() throws Exception {
		PartitionDimensionDao dao = new PartitionDimensionDao(getDataSource(getHiveDatabaseName()));
		assertEquals(0, dao.loadAll().size());
		final PartitionDimension d = createEmptyPartitionDimension();
		dao.create(d);
		assertTrue(d.getId()>0);
		assertEquals(1, dao.loadAll().size());
	}
}
