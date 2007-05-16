package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.sql.SQLException;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.database.HiveTestCase;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JdbcDaoSupportCacheTest extends HiveTestCase {
	protected boolean cleanupDbAfterEachTest = true;
	
	@BeforeClass
	public void setUp() throws Exception{
		new HiveInstaller(getConnectString()).run();
		Hive hive = Hive.load(getConnectString());
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		new IndexSchema(hive.getPartitionDimension(partitionDimensionName())).install();
		hive.addNode(hive.getPartitionDimension(partitionDimensionName()), createNode());
		hive.insertPrimaryIndexKey(partitionDimensionName(), intKey());
	}
	
	@Test
	public void testDataSourceCacheCreation() throws HiveException, SQLException{
		Hive hive = Hive.load(getConnectString());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.getJdbcDaoSupportCache(partitionDimensionName());
		JdbcDaoSupport read = cache.get(intKey(), AccessType.Read);
		JdbcDaoSupport readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		assertNotNull(read);
		assertNotNull(readWrite);
	}

	@Test
	public void testReadOnlyEnforcement() throws Exception {
		Hive hive = Hive.load(getConnectString());
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.getJdbcDaoSupportCache(partitionDimensionName());
		JdbcDaoSupport read = cache.get(intKey(), AccessType.Read);
		JdbcDaoSupport readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		readWrite.getJdbcTemplate().update("create table BAR (name varchar(50))");

		try {
		read.getJdbcTemplate().update("insert into BAR values ('not foo')");
		} catch(RuntimeException e) {
			assertNotNull(e);
		}
		readWrite.getJdbcTemplate().update("insert into BAR values ('foo')");
		assertEquals(1, read.getJdbcTemplate().queryForInt("select count(1) from BAR"));
	}
	
	public void testCacheSynchronization() throws Exception {
		Hive hive = Hive.load(getConnectString());
		PartitionDimension dimension = hive.deletePartitionDimension(hive.getPartitionDimension(partitionDimensionName()));
		try {
			hive.getJdbcDaoSupportCache(dimension);
		} catch (Exception e) {
			assertNotNull(e);
		}
	}
	
	private Integer intKey() {
		return new Integer(23);
	}
	
}
