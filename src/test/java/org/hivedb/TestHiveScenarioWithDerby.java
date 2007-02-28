package org.hivedb;

import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.HiveScenario;
import org.hivedb.util.HiveScenarioAlternativeConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestHiveScenarioWithDerby extends DerbyTestCase {	
	@BeforeMethod
	public void setUp() {
		// must not share Derby across methods for this test
		cleanupDerby();
		initializeDerby();		
	}
	
	/**
	 *  Fills a hive with metaday and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
	@Test
	public void testPirateDomain() throws Exception {
		HiveScenarioTests.performTest(databaseName, new HiveScenario.HiveScenarioConfig(), getConnectString());
	}

	/**
	 *  An alternative object model
	 */
	@Test
	public void testMemberDomain() throws Exception {
		HiveScenarioTests.performTest(databaseName, new HiveScenarioAlternativeConfig(), getConnectString());
	}
}
