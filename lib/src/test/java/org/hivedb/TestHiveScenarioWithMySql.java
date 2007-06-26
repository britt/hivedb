package org.hivedb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.database.HiveMySqlTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.Test;

public class TestHiveScenarioWithMySql extends HiveMySqlTestCase {

	/**
	 *  Fills a hive with metadata and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
	@Test(groups={"mysql"})
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(getHiveDatabaseName()), getDataUris())).performTest(10,10);
	}
	private Collection<String> getDataUris() {
		Collection<String> uris = new ArrayList<String>();
		for(String name : getDataNodeNames())
			uris.add(getConnectString(name));
		return uris;
	}
	
	private Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"test_data1","test_data2","test_data3"});
	}

	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{getHiveDatabaseName(),"test_data1","test_data2","test_data3"});
	}
	@Override
	public String getHiveDatabaseName() {
		return "storage_test";
	}
}
