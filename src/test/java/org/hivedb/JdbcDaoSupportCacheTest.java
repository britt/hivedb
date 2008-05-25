package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.meta.persistence.CachingDataSourceProvider;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.database.test.HiveTest;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcDaoSupportCacheTest extends HiveTest {
	protected boolean cleanupDbAfterEachTest = true;
	
	@Test
	public void testDataSourceCacheCreation() throws HiveException, SQLException{
		getHive().directory().insertPrimaryIndexKey(intKey());
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		Collection<SimpleJdbcDaoSupport> read = cache.get(intKey(), AccessType.Read);
		Collection<SimpleJdbcDaoSupport> readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		assertTrue(read.size() > 0);
		assertTrue(readWrite.size() > 0);
	}
	
	@Test
	public void testGetAllUnsafe() throws Exception {
		getHive().directory().insertPrimaryIndexKey(intKey());
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()), CachingDataSourceProvider.getInstance());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		assertEquals(3, cache.getAllUnsafe().size());
	}
	
	private Integer intKey() {
		return new Integer(23);
	}

	
}
