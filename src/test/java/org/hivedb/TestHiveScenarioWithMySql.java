package org.hivedb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hivedb.meta.Node;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.MySqlHiveTestCase;
import org.hivedb.util.functional.Transform;
import org.testng.annotations.Test;

public class TestHiveScenarioWithMySql extends MySqlHiveTestCase {
	
	@Test(groups={"mysql"})
	public void test()  throws Exception {
		for (String nodeName : getDataNodeNames())
			getHive().addNode(new Node(Hive.NEW_OBJECT_ID, nodeName, getHiveDatabaseName(), "", HiveDbDialect.H2));
		new TestHiveScenario(getEntityHiveConfig(), getHive()).test();
	}

	protected Collection<String> getDataNodeNames() {
		return Arrays.asList(new String[]{"data1","data2","data3"});
	}

}
