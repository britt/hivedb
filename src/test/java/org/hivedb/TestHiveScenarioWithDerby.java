package org.hivedb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;

public class TestHiveScenarioWithDerby extends HiveTestCase {
	
	/**
	 *  Fills a hive with metadata and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */
//	@Test
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(getHiveDatabaseName()), getDataUris())).performTest(100,0);
	}
	private Collection<String> getDataUris() {
		Collection<String> uris = new ArrayList<String>();
		for(String name : getDataNodeNames())
			uris.add(getConnectString(name));
		return uris;
	}
	
	private Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}

	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{getHiveDatabaseName(),"data1","data2","data3"});
	}
}
