package org.hivedb.persistence;

import static org.testng.AssertJUnit.assertEquals;

import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.testng.annotations.Test;

@Config(file="hive_default")
public class TestHiveSemaphorePersistence extends HiveTest {
	
	@Test
	public void testUpdate() throws Exception {
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(getConnectString(getHiveDatabaseName())));
		HiveSemaphore hs = hsd.create();
		hs.incrementRevision();
		hsd.update(hs);
		
		HiveSemaphore hs2 = hsd.get();
		assertEquals(hs.getRevision(),hs2.getRevision());
		assertEquals(hs.getStatus(),hs2.getStatus());
	}

}
