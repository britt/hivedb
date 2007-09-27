package org.hivedb;

import java.util.Collection;

import org.hivedb.meta.Node;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfigForResourceAndPartitionDimensionEntity;
import org.hivedb.util.scenarioBuilder.HiveScenarioConfigForResourceEntity;

public class TestHiveScenario {
	
	String hiveUri;
	Collection<Node> dataNodes;
	public TestHiveScenario(String hiveUri, Collection<Node> dataNodes) {
		this.hiveUri = hiveUri;
		this.dataNodes = dataNodes;
	}
	
	public void testResourceEntity() {
		new HiveScenarioTest(
						new HiveScenarioConfigForResourceEntity(
								Hive.load(hiveUri),
								dataNodes))
									.performTest(10,20);
	}
	public void testResourceAndPartitionDimensionEntity() {
		new HiveScenarioTest(
						new HiveScenarioConfigForResourceAndPartitionDimensionEntity(
								Hive.load(hiveUri),
								dataNodes))
									.performTest(10,10);
	}
	

	

}
