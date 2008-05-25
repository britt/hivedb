package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.sql.Types;
import java.util.ArrayList;

import org.hivedb.Hive;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.Resource;
import org.hivedb.meta.SecondaryIndex;
import org.hivedb.meta.persistence.SecondaryIndexDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.HiveTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSecondaryIndexPersistence extends HiveTest {
	
	@Test
	public void testCreate() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getConnectString(getHiveDatabaseName())));
		int initialSize = d.loadAll().size();
		d.create(createSecondaryIndex());
		assertEquals(initialSize+1,d.loadAll().size());
	}
	
	@Test
	public void testDelete() throws Exception {
		SecondaryIndexDao d = new SecondaryIndexDao(getDataSource(getConnectString(getHiveDatabaseName())));
		int initialSize = d.loadAll().size();
		int id = d.create(createSecondaryIndex());
		assertEquals(initialSize+1,d.loadAll().size());
		SecondaryIndex s = createSecondaryIndex(id);
		s.setResource(createResource());
		d.delete(s);
		assertEquals(initialSize,d.loadAll().size());
	}
}
