package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import javax.sql.DataSource;

import org.hivedb.management.KeyAuthority;
import org.hivedb.management.MySqlKeyAuthority;
import org.hivedb.util.database.HiveDbDialect;
import org.hivedb.util.database.test.HiveTest;

public class TestMysqlKeyAuthority extends HiveTest {
	DataSource ds = null;

	public HiveDbDialect getDialect() {
		return HiveDbDialect.MySql;
	}
	
	//@Test(groups={"mysql"})
	@SuppressWarnings("unchecked")
	public void testAssign() throws Exception {
		KeyAuthority authority = new MySqlKeyAuthority(
				getDataSource(getHive().getUri()), this.getClass(), Integer.class);
		int firstKey = (Integer) authority.nextAvailableKey();
		int secondKey = (Integer) authority.nextAvailableKey();
		assertTrue(secondKey > firstKey);
	}
}
