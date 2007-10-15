package org.hivedb.persistence;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.HiveScenarioTest;
import org.hivedb.meta.AccessType;
import org.hivedb.util.InstallHiveIndexSchema;
import org.hivedb.util.database.H2HiveTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import bsh.Console;

public class TestHiveDuplicatePrimaryIndexInsert  extends H2HiveTestCase {
	
	/**
	 * Tests that primary index keys can only be associated with one node
	 * This functionality should be configurable in the future to allow
	 * multiple nodes.
	 *
	 */
	@Test
	public void testDuplicatePrimaryIndexKeyInsert() {
		try {
			HiveScenarioMarauderConfig hiveScenarioConfig = new HiveScenarioMarauderConfig(getConnectString(getHiveDatabaseName()), getDataUris());
			InstallHiveIndexSchema.install(hiveScenarioConfig, hiveScenarioConfig.getHive());
			String partitionDimensionName = hiveScenarioConfig.getPrimaryIndexIdentifiable().getPartitionDimensionName();
			Hive hive = hiveScenarioConfig.getHive();
			// Insert the same primary index key multiple times to assure that only one
			// node ever is assigned.
			
			// Healthy inserts with different ids
			for (int i=0; i<20; i++) // make chance of no distinct node choice statistically unlikeley			
				hive.insertPrimaryIndexKey(partitionDimensionName, i);
			int numberOfDistinctNodesChosen=0;
			for (int i=0; i<20; i++) // make chance of no distinct node choice statistically unlikeley
				try {
					hive.insertPrimaryIndexKey(partitionDimensionName, 10000);
					numberOfDistinctNodesChosen++;
				}
				catch (Exception e) {}
			// use our number of successful inserts to see how many times a unique node
			// was chosen. It must be at least once for the test to be meaningful
			Assert.assertTrue(numberOfDistinctNodesChosen > 1);
			// Assert that although different nodes were chosen only one was persisted
			Collection<Connection> connections = hive.getConnection(partitionDimensionName, 10000, AccessType.Read);
			Assert.assertEquals(connections.size(), 1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
