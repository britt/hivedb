package org.hivedb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.Test;

public class TestHiveScenarioWithH2 extends H2HiveTestCase {
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
				return new Node(0, dataNodeName, dataNodeName, "", 0, HiveDbDialect.H2);
		}},
		getDataNodeNames());
	}
	private Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}
	@SuppressWarnings("unchecked")
	public Collection<String> getDatabaseNames() {
		return Transform.flatten(Collections.singletonList(getHiveDatabaseName()),getDataNodeNames());
	}
}
