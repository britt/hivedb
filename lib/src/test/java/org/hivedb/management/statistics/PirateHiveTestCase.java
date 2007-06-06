package org.hivedb.management.statistics;

import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenario;


public abstract class PirateHiveTestCase extends HiveTestCase {
	protected HiveScenario yeScenario = null;
	
	public void setUp() {
//		try {
//			yeScenario = HiveScenario.run(new HiveScenarioMarauderConfig(getConnectString(), dataNodes), 100, 10);
//		} catch (Exception e) {
//			e.printStackTrace();
//			fail("Unable to initialize the hive: " + e.getMessage());
//		} 
	}
}
