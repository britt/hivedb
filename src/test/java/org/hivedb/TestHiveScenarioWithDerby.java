package org.hivedb;

import java.util.Collection;

import org.hivedb.util.database.DerbyTestCase;
import org.hivedb.util.scenarioBuilder.HiveScenarioMarauderConfig;
import org.testng.annotations.BeforeMethod;

public class TestHiveScenarioWithDerby extends DerbyTestCase {
	
	private Collection<String> dataNodes = null;
	@BeforeMethod
	public void setup() {
//		this.beforeMethod();
//		this.dataNodes = Transform.map(new Unary<Number, String>() {
//			public String f(Number count) { 
//				try {
//					DerbyUtils.createDatabase("data"+count, userName, password);
//					return "data"+count;
//				} catch (Exception e) {
//					throw new RuntimeException(e);
//				}
//			}},
//			new NumberIterator(3));
//		new HiveInstaller(getConnectString()).run();
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
