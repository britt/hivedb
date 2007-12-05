package org.hivedb;

import org.hivedb.util.database.test.MySqlHiveTestCase;
import org.testng.annotations.Test;

public class TestHiveScenarioWithMySql extends MySqlHiveTestCase {
	
	@Test(groups={"mysql"})
	public void test()  throws Exception {
		new TestHiveScenario(getEntityHiveConfig(), getHive()).test();
	}
}
