package org.hivedb.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.Hive;
import org.hivedb.meta.HiveConfig;
import org.hivedb.meta.Node;
import org.hivedb.util.database.test.H2HiveTestCase;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfigForResourceEntity;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;
import org.testng.annotations.Test;

public class TestHiveUpdater extends H2HiveTestCase {
	@Test
	public void testHiveUpdater() {
		HiveConfig hiveConfig = new HiveScenarioConfigForResourceEntity(
			Hive.load(getConnectString(getHiveDatabaseName())),
			getDataNodes());
		new HiveScenarioTest(hiveConfig)
			.performTest(10, 2, new PersisterImpl());
		
	}
	private Collection<Node> getDataNodes() {
		return Transform.map(new Unary<String, Node>() {
			public Node f(String dataNodeName) {
				return new Node(0, dataNodeName, getConnectString(dataNodeName), cleanupAfterEachTest, 0);
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
}
