package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResourcePersistence extends HiveTestCase {
	public TestResourcePersistence() {
		this.cleanupOnLoad = true;
		this.cleanupDbAfterEachTest = true;
	}
	
	@BeforeMethod
	public void setUp() throws Exception {
		super.beforeMethod();
	}
	
	@Test
	public void testCreate() throws Exception {
		ds = new HiveBasicDataSource(getConnectString());
		new HiveInstaller(getConnectString()).run();
		ResourceDao d = new ResourceDao(ds);
		d.setDataSource(ds);
		d.create(createResource());
		assertEquals(1,d.loadAll().size());
	}
}
