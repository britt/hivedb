package org.hivedb;

import org.hivedb.util.database.MysqlTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeMethod;

public class TestHiveScenarioWithMySql extends MysqlTestCase {
	
	@BeforeMethod
	public void setup() {
		super.setUp();
	}

	/**
	 *  Fills a hive with metadata and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
//	@Test(groups={"mysql"})
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(), getDataUris())).performTest(100,0);
	}
}
