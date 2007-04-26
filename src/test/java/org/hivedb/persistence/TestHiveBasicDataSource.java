package org.hivedb.persistence;

import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.DerbyTestCase;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHiveBasicDataSource extends DerbyTestCase{

	@Test
	public void testPoolSize() {
		HiveBasicDataSource ds = new HiveBasicDataSource(getConnectString());
		AssertJUnit.assertEquals(HiveBasicDataSource.DEFAULT_POOL_SIZE, ds.getMaxActive());
		
		System.setProperty(HiveBasicDataSource.CONNECTION_POOL_SIZE, "20");
		HiveBasicDataSource ds20 = new HiveBasicDataSource(getConnectString());
		AssertJUnit.assertEquals(20, ds20.getMaxActive());
		
//		System.out.println("MaxActive: " + ds.getMaxActive());
//		System.out.println("MaxIdle: " + ds.getMaxIdle());
//		System.out.println("MaxOpenPreparedStatements: " + ds.getMaxOpenPreparedStatements());
//		System.out.println("MaxWait: " + ds.getMaxWait());
//		System.out.println("InitialSize: " + ds.getInitialSize());
//		System.out.println("MinIdle: " + ds.getMinIdle());
	}
}
