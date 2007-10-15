package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.Collection;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcDaoSupportCacheTest extends H2HiveTestCase {
	protected boolean cleanupDbAfterEachTest = true;
	
	@BeforeMethod
	public void setUp() throws Exception{
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		new IndexSchema(hive.getPartitionDimension(partitionDimensionName())).install();
		hive.addNode(hive.getPartitionDimension(partitionDimensionName()), createNode(getHiveDatabaseName()));
		hive.insertPrimaryIndexKey(partitionDimensionName(), intKey());
	}
	
	@Test
	public void testDataSourceCacheCreation() throws HiveException, SQLException{
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.getJdbcDaoSupportCache(partitionDimensionName());
		Collection<SimpleJdbcDaoSupport> read = cache.get(intKey(), AccessType.Read);
		Collection<SimpleJdbcDaoSupport> readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		assertTrue(read.size() > 0);
		assertTrue(readWrite.size() > 0);
	}
	
	@Test
	public void testGetAllUnsafe() {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.getJdbcDaoSupportCache(partitionDimensionName());
		assertEquals(1, cache.getAllUnsafe().size());
	}
	
	public void testCacheSynchronization() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		PartitionDimension dimension = hive.deletePartitionDimension(hive.getPartitionDimension(partitionDimensionName()));
		try {
			hive.getJdbcDaoSupportCache(dimension.getName());
		} catch (Exception e) {
			assertNotNull(e);
		}
	}
	
	private Integer intKey() {
		return new Integer(23);
	}

	
}
