package org.hivedb;

import java.util.Collection;

import org.hivedb.util.DerbyTestCase;
import org.hivedb.util.database.DerbyUtils;
import org.hivedb.util.functional.NumberIterator;
import org.hivedb.util.functional.Transform;
import org.hivedb.util.functional.Unary;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

public class TestHiveScenarioWithDerby extends DerbyTestCase {
	
	private Collection<String> dataNodes = null;
	@BeforeMethod
	public void setup() {

		this.dataNodes = Transform.map(new Unary<Number, String>() {
			public String f(Number count) { 
				try {
					DerbyUtils.createDatabase("data"+count, userName, password);
					return "data"+count;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}},
			new NumberIterator(3));
	}
	
	@BeforeTest
	public void before() {
		cleanupDbAfterEachTest = true;
	}
	/**
	 *  Fills a hive with metaday and indexes to validate CRUD operations
	 *  This tests works but is commented out due to its slowness
	 */

	//TODO fix
//	@Test
	public void testPirateDomain() throws Exception {
		new HiveScenarioTest(new HiveScenarioMarauderConfig(getConnectString(), dataNodes)).performTest(100,10);
	}

}
