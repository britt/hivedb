package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.HiveTestCase;
import org.testng.annotations.Test;

public class TestResourcePersistence extends HiveTestCase {
	@Test
	public void testCreate() throws Exception {
		ResourceDao d = new ResourceDao(ds);
		d.setDataSource(ds);
		d.create(createResource());
		assertEquals(1,d.loadAll().size());
	}
}
