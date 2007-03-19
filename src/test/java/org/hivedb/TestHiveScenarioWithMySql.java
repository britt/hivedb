package org.hivedb;

import org.hivedb.util.MysqlTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioAlternativeConfig;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeTest;

public class TestHiveScenarioWithMySql extends MysqlTestCase {
	
	private String database = "test";
	@BeforeTest
	public void setUp() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			recycleDatabase(database, getDatabaseAgnosticConnectString());
		} 
		catch (Exception e) { throw new RuntimeException("Failed to load driver class"); }
	}
	
	/**
	 *  Fills a hive with metaday and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
	//@Test(groups={"mysql"})
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(database))).performTest();
	}

	/**
	 *  An alternative object model
	 */
	//@Test(groups={"mysql"})
	public void testMemberDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioAlternativeConfig(getConnectString(database))).performTest();
	}
}
