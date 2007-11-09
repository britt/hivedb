package org.hivedb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveScenarioTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.Test;

public class TestHiveScenarioWithH2 extends H2HiveScenarioTestCase {
	@Test
	public void test() {
		for (SingularHiveConfig singularHiveConfig : getHiveConfigs())
			new TestHiveScenario(singularHiveConfig).test();
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
