package org.hivedb;

import org.hivedb.util.database.test.H2HiveTestCase;
import org.testng.annotations.Test;

public class TestHiveScenarioWithH2 extends H2HiveTestCase {
	@Test
	public void test() throws Exception {
		new TestHiveScenario(getEntityHiveConfig(), getMappedClasses(), getHive()).test();
	}
}
