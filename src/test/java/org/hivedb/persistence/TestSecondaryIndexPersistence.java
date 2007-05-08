package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.database.HiveTestCase;
import org.testng.annotations.Test;

public class TestSecondaryIndexPersistence extends HiveTestCase {
	@Test
	public void testCreate() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(ds);
		assertEquals(0,d.loadAll().size());
		d.create(createSecondaryIndex());
		assertEquals(1,d.loadAll().size());
	}
	@Test
	public void testDelete() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(ds);
		assertEquals(0,d.loadAll().size());
		int id = d.create(createSecondaryIndex());
		assertEquals(1,d.loadAll().size());
		SecondaryIndex s = createSecondaryIndex(id);
		s.setResource(createResource());
		d.delete(s);
		assertEquals(0,d.loadAll().size());
	}
}
