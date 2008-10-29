package org.hivedb;

import org.hivedb.AccessType;
import org.hivedb.persistence.JdbcDaoSupportCacheImpl;
import org.hivedb.util.database.test.HiveTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;

import java.sql.SQLException;
import java.util.Collection;

public class JdbcDaoSupportCacheTest extends HiveTest {
	protected boolean cleanupDbAfterEachTest = true;
	
	@Test
	public void testDataSourceCacheCreation() throws HiveException, SQLException{
		getHive().directory().insertPrimaryIndexKey(intKey());
		Hive hive = null;//Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		Collection<SimpleJdbcDaoSupport> read = cache.get(intKey(), AccessType.Read);
		Collection<SimpleJdbcDaoSupport> readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		assertTrue(read.size() > 0);
		assertTrue(readWrite.size() > 0);
	}
	
	@Test
	public void testGetAllUnsafe() throws Exception {
		getHive().directory().insertPrimaryIndexKey(intKey());
		Hive hive = null;//Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		assertEquals(3, cache.getAllUnsafe().size());
	}
	
	private Integer intKey() {
		return new Integer(23);
	}

	
}
