package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestHiveLoading extends H2TestCase {

	@BeforeMethod
	public void setup() throws Exception {
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
	}
	
	@Test
	public void testLoadingWithoutPartitionDimension() throws Exception {
		Hive hive = Hive.load(getConnectString(H2TestCase.TEST_DB));
		assertNotNull(hive);
		Node node = getNode();
		hive.addNode(node);
		
		Hive fetched = Hive.load(getConnectString(H2TestCase.TEST_DB));
		assertEquals(1, fetched.getNodes().size());
		assertEquals(node, fetched.getNode(node.getName()));
	}

	private Node getNode() {
		return new Node(Hive.NEW_OBJECT_ID, "nodal", H2TestCase.TEST_DB, "", HiveDbDialect.H2);
	}
	
	@Test
	public void testLoadWithPartitionDimension() throws Exception {
		Hive hive = Hive.create(getConnectString(H2TestCase.TEST_DB), "DIM", Types.INTEGER);
		assertNotNull(hive);
		Node node = getNode();
		hive.addNode(node);
		
		Hive fetched = Hive.load(getConnectString(H2TestCase.TEST_DB));
		assertEquals(1, fetched.getNodes().size());
		assertEquals(node, fetched.getNode(node.getName()));
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Collections.singletonList(H2TestCase.TEST_DB);
	}
	
	
}
