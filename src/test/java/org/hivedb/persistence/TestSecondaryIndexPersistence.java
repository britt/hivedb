package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSecondaryIndexPersistence extends H2HiveTestCase {
	
	@BeforeMethod
	@Override
	public void beforeMethod() {
		deleteDatabasesAfterEachTest = true;
		super.afterMethod();
		super.beforeMethod();
	}
	
	@Test
	public void testCreate() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getHiveDatabaseName()));
		int initialSize = d.loadAll().size();
		d.create(createSecondaryIndex());
		assertEquals(initialSize+1,d.loadAll().size());
	}
	@Test
	public void testDelete() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getHiveDatabaseName()));
		int initialSize = d.loadAll().size();
		int id = d.create(createSecondaryIndex());
		assertEquals(initialSize+1,d.loadAll().size());
		SecondaryIndex s = createSecondaryIndex(id);
		s.setResource(createResource());
		d.delete(s);
		assertEquals(initialSize,d.loadAll().size());
	}

}
