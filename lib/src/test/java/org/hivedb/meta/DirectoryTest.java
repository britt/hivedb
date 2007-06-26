package org.hivedb.meta;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.hivedb.Hive;
import org.hivedb.meta.persistence.HiveBasicDataSource;
import org.hivedb.util.database.HiveTestCase;
import org.hivedb.util.functional.Atom;
import org.hivedb.util.scenarioBuilder.HiveScenario;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DirectoryTest extends HiveTestCase {
	@BeforeMethod
	public void setUp() throws Exception {
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
		hive.addNode(Atom.getFirst(hive.getPartitionDimensions()), createNode(getHiveDatabaseName()));
		new IndexSchema(Atom.getFirst(hive.getPartitionDimensions())).install();
	}
	
	@SuppressWarnings("unchecked")
//	@Test
	public void testGetNodeIdsOfSecondaryIndexKeys() throws Exception {
		HiveScenario hiveScenario = HiveScenario.run(new HiveScenarioMarauderConfig(getConnectString(getHiveDatabaseName()), getDataUris()), 10, 10);
		Hive hive = Hive.load(getConnectString(getHiveDatabaseName()));
		hive.addPartitionDimension(createPopulatedPartitionDimension());
	}
	
	private Collection<String> getDataUris() {
		Collection<String> uris = new ArrayList<String>();
		for(String name : getDatabaseNames())
			uris.add(getConnectString(name));
		return uris;
	}
}
