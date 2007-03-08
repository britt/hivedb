package org.hivedb;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.hivedb.util.scenarioBuilder.HiveScenarioAlternativeConfig;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestHiveScenarioWithMySql {
	
	@BeforeTest
	public void setUp() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} 
		catch (Exception e) { throw new RuntimeException("Failed to load driver class"); }
	}

	public String getDatabaseAgnosticConnectString() {
		return "jdbc:mysql://localhost/?user=test&password=test";
	}
	public String getConnectString(String database) {
		return "jdbc:mysql://localhost/"+database+"?user=test&password=test";
	}
	
	/**
	 *  Fills a hive with metaday and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
	//@Test(groups={"mysql"})
	public void testPirateDomain() throws Exception {
		String databaseName = "test";
		recycleDatabase(databaseName, getDatabaseAgnosticConnectString());
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(databaseName))).performTest();
	}

	/**
	 *  An alternative object model
	 */
	//@Test(groups={"mysql"})
	public void testMemberDomain() throws Exception {
		String databaseName = "alternative_test";
		recycleDatabase(databaseName, getDatabaseAgnosticConnectString());
		new HiveScenarioTest(new HiveScenarioAlternativeConfig(getConnectString(databaseName))).performTest();
	}
	
	private static void recycleDatabase(String databaseName, String databaseAgnosticConnectionString) throws SQLException {
		java.sql.Connection connection  = DriverManager.getConnection( databaseAgnosticConnectionString );
		try {
			connection.prepareStatement("drop database " + databaseName).execute();
		}
		catch (Exception e) {}
		connection.prepareStatement("create database " + databaseName).execute();
		connection.close();
	}
}
