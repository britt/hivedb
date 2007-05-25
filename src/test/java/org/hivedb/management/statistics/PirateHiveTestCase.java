package org.hivedb.management.statistics;

import static org.testng.AssertJUnit.fail;

import java.util.Collection;

import org.apache.commons.dbcp.BasicDataSource;
import org.hivedb.meta.GlobalSchema;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.meta.persistence.HiveSemaphoreDao;
import org.hivedb.util.database.DerbyTestCase;
import org.hivedb.util.database.DerbyUtils;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;


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
