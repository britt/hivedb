package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.Collection;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.IndexSchema;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcDaoSupportCacheTest extends HiveTestCase {
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
	public void testReadOnlyEnforcement() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.getJdbcDaoSupportCache(partitionDimensionName());
		Collection<SimpleJdbcDaoSupport> read = cache.get(intKey(), AccessType.Read);
		Collection<SimpleJdbcDaoSupport> readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		JdbcDaoSupport readWriteDao = Atom.getFirst(readWrite);
		JdbcDaoSupport readDao = Atom.getFirst(read);
		readWriteDao.getJdbcTemplate().update("create table BAR (name varchar(50))");

		try {
			readDao.getJdbcTemplate().update("insert into BAR values ('not foo')");
		} catch(RuntimeException e) {
			assertNotNull(e);
		}
		readWriteDao.getJdbcTemplate().update("insert into BAR values ('foo')");
		assertEquals(1, readDao.getJdbcTemplate().queryForInt("select count(1) from BAR"));
	}
	
	public void testCacheSynchronization() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
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
