package org.hivedb;

import org.hivedb.configuration.SingularHiveConfig;
import org.hivedb.util.PersisterImpl;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;

public class TestHiveScenario {
	
	SingularHiveConfig singularHiveConfig;
	public TestHiveScenario(SingularHiveConfig singularHiveConfig) {
		this.singularHiveConfig = singularHiveConfig;
	}
	
	public void test() {
		int resourceInstanceCount = singularHiveConfig.getEntityConfig().isPartitioningResource()
			? 2 // must equal primaryIndexKeyCount
			: 4; // make greater so that primaryIndexKeys are shared
		new HiveScenarioTest(singularHiveConfig).performTest(2,resourceInstanceCount, new PersisterImpl());
	}
}