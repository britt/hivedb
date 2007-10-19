package org.hivedb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.HiveMySqlTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.Test;

public class TestHiveScenarioWithMySql extends HiveMySqlTestCase {

	@Test
	public void testResourceOnlyEntity() {
		new TestHiveScenario(getConnectString(getHiveDatabaseName()), getDataNodes()).testResourceEntity();
	}
	@Test
	public void testResourceAndPartitionDimensionEntity() {
		new TestHiveScenario(getConnectString(getHiveDatabaseName()), getDataNodes()).testResourceAndPartitionDimensionEntity();
	}
	
	private Collection<Node> getDataNodes() {
		return Transform.map(new Unary<String, Node>() {
			public Node f(String dataNodeName) {
				return new Node(0, dataNodeName, dataNodeName, "localhost", 0, HiveDbDialect.MySql);
		}},
		getDataNodeNames());
	}
	private Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}
	@SuppressWarnings("unchecked")
	@Override
	public Collection<String> getDatabaseNames() {
		return Transform.flatten(Collections.singletonList(getHiveDatabaseName()),getDataNodeNames());
	}
	
	@Override
	public String getHiveDatabaseName() {
		return "storage_test";
	}

}
