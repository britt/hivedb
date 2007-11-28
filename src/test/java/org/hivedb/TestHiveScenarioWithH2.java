package org.hivedb;

import java.util.Arrays;
import java.util.Collection;

import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.testng.annotations.Test;

public class TestHiveScenarioWithH2 extends H2HiveTestCase {
	@Test
	public void test() throws Exception {
		new TestHiveScenario(getEntityHiveConfig()).test();
	}
	
	private Collection<Node> getDataNodes() {
		return Transform.map(new Unary<String, Node>() {
			public Node f(String dataNodeName) {
				return new Node(0, dataNodeName, dataNodeName, "", HiveDbDialect.H2);
		}},
		getDataNodeNames());
	}
	protected Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}
	
}
