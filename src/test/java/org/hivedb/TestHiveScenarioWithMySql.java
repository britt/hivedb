package org.hivedb;

import java.util.Collection;

import org.hivedb.util.MysqlTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeMethod;

public class TestHiveScenarioWithMySql extends MysqlTestCase {
	
	private Collection<String> dataNodes = null;
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
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(), dataNodes)).performTest(100,0);
	}
}
