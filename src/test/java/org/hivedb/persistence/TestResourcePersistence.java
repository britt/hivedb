package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Resource;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResourcePersistence extends H2HiveTestCase {
	@BeforeMethod
	public void setUp() throws Exception {
		super.beforeMethod();
	}
	
	@Test
	public void testCreate() throws Exception {
		ResourceDao d = new ResourceDao(getDataSource(getHiveDatabaseName()));
		final Resource createResource = createResource();
		d.create(createResource);
		assertEquals(1,d.loadAll().size());
	}
}
