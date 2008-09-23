package org.hivedb.persistence;

import org.hivedb.meta.HiveSemaphore;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.database.test.HiveTest.Config;
import org.junit.Test;import org.junit.Assert;

@Config("hive_default")
public class TestHiveSemaphorePersistence extends HiveTest {
	
	@Test
	public void testUpdate() throws Exception {
		HiveSemaphoreDao hsd = new HiveSemaphoreDao(getDataSource(getConnectString(getHiveDatabaseName())));
		HiveSemaphore hs = hsd.create();
		hs.incrementRevision();
		hsd.update(hs);
		
		HiveSemaphore hs2 = hsd.get();
		Assert.assertEquals(hs.getRevision(),hs2.getRevision());
		Assert.assertEquals(hs.getStatus(),hs2.getStatus());
	}

}
