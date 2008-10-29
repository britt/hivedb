package org.hivedb.persistence;

import org.hivedb.SecondaryIndex;
import org.hivedb.configuration.persistence.SecondaryIndexDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.junit.Test;import static org.junit.Assert.assertEquals;

@Config("hive_default")
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
