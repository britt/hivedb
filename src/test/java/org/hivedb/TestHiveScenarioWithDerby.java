package org.hivedb;

import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMemberConfig;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestHiveScenarioWithDerby extends DerbyTestCase {
	
	@BeforeTest
	public void before() {
		cleanupDbAfterEachTest = true;
	}
	/**
	 *  Fills a hive with metaday and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */

	//TODO fix
	//@Test
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString())).performTest();
	}

	/**
	 *  An alternative object model
	 */
	@Test
	public void testMemberDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMemberConfig(getConnectString())).performTest();
	}
}
