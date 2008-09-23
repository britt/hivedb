package org.hivedb.persistence;

import org.hivedb.meta.persistence.ResourceDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.junit.Test;import static org.junit.Assert.assertEquals;

@Config("hive_default")
public class TestResourcePersistence extends HiveTest {
	
	@Test
	public void testCreate() throws Exception {
		ResourceDao d = new ResourceDao(getDataSource(getConnectString(getHiveDatabaseName())));
		int intitialSize = d.loadAll().size();
		d.create(createResource());
		assertEquals(intitialSize+1,d.loadAll().size());
	}
}
