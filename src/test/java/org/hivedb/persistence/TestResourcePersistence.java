package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.Resource;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResourcePersistence extends H2HiveTestCase {
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
	}
	
	@Test
	public void testCreate() throws Exception {
		ResourceDao d = new ResourceDao(getDataSource(getHiveDatabaseName()));
		final Resource createResource = createResource();
		int intitialSize = d.loadAll().size();
		d.create(createResource);
		assertEquals(intitialSize+1,d.loadAll().size());
	}
}
