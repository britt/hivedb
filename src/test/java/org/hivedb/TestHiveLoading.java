package org.hivedb;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.configuration.HiveConfigurationSchema;
import org.hivedb.management.HiveInstaller;
import org.hivedb.meta.Node;
import org.hivedb.meta.PartitionDimension;
import org.hivedb.meta.PartitionDimensionCreator;
import org.hivedb.meta.persistence.IndexSchema;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2TestCase;
import org.hivedb.util.functional.Transform;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestHiveLoading extends H2TestCase {

	Hive hive;
	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();
		new HiveInstaller(getConnectString(H2TestCase.TEST_DB)).run();
		hive = Hive.load(getConnectString(H2TestCase.TEST_DB));
	}
	public Collection<Schema> getSchemas() {
		return Arrays.asList(new Schema[] {
				new HiveConfigurationSchema(getConnectString(H2TestCase.TEST_DB))});
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
