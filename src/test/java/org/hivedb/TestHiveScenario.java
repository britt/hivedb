package org.hivedb;

import org.hivedb.configuration.EntityConfig;
import org.hivedb.configuration.EntityHiveConfig;
import org.hivedb.configuration.SingularHiveConfigImpl;
import org.hivedb.util.PersisterImpl;
import org.hivedb.util.scenarioBuilder.HiveScenarioTest;

public class TestHiveScenario {
	
	EntityHiveConfig enitityHiveConfig;
	public TestHiveScenario(EntityHiveConfig enitityHiveConfig) {
		this.enitityHiveConfig = enitityHiveConfig;
	}
	
	public void test() {
		for (EntityConfig entityConfig : enitityHiveConfig.getEntityConfigs()) {
			int resourceInstanceCount = entityConfig.isPartitioningResource()
				? 2 // must equal primaryIndexKeyCount
				: 4; // make greater so that primaryIndexKeys are shared
			new HiveScenarioTest(new SingularHiveConfigImpl(enitityHiveConfig.getHive(), entityConfig)).performTest(2,resourceInstanceCount, new PersisterImpl());
		}
	}
}