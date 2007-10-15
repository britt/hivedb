package org.hivedb.persistence;
import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.Test;

public class TestHiveSemaphorePersistence extends H2HiveTestCase {
	
	@Test
	public void testUpdate() throws Exception {
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(getHiveDatabaseName()));
		HiveSemaphore hs = hsd.create();
		hs.incrementRevision();
		hsd.update(hs);
		
		HiveSemaphore hs2 = hsd.get();
		assertEquals(hs.getRevision(),hs2.getRevision());
		assertEquals(hs.isReadOnly(),hs2.isReadOnly());
	}

}
