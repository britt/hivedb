package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.AccessType;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcDaoSupportCacheTest extends H2HiveTestCase {
	protected boolean cleanupDbAfterEachTest = true;
	
	@BeforeMethod
	public void setUp() throws Exception{
		new HiveInstaller(getConnectString(getHiveDatabaseName())).run();
		
		for (String nodeName : getDataNodeNames())
			getHive().addNode(new Node(Hive.NEW_OBJECT_ID, nodeName, getHiveDatabaseName(), "", Hive.NEW_OBJECT_ID, HiveDbDialect.H2));
		getHive().directory().insertPrimaryIndexKey(intKey());
	}
	protected Collection<Node> getDataNodes() {
		return Transform.map(new Unary<String, Node>() {
			public Node f(String dataNodeName) {
				return new Node(0, dataNodeName, dataNodeName, "", 0, HiveDbDialect.H2);
		}},
		getDataNodeNames());
	}
	protected Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}
	
	@Test
	public void testDataSourceCacheCreation() throws HiveException, SQLException{
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		Collection<SimpleJdbcDaoSupport> read = cache.get(intKey(), AccessType.Read);
		Collection<SimpleJdbcDaoSupport> readWrite = cache.get(intKey(), AccessType.ReadWrite);
		
		assertTrue(read.size() > 0);
		assertTrue(readWrite.size() > 0);
	}
	
	@Test
	public void testGetAllUnsafe() {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		JdbcDaoSupportCacheImpl cache = (JdbcDaoSupportCacheImpl) hive.connection().daoSupport();
		assertEquals(3, cache.getAllUnsafe().size());
	}
	private Integer intKey() {
		return new Integer(23);
	}

	
}
