package org.hivedb;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import javax.sql.DataSource;

import org.hivedb.management.KeyAuthority;
import org.hivedb.management.MySqlKeyAuthority;
import org.hivedb.util.database.test.HiveMySqlTestCase;
import org.testng.annotations.Test;

public class TestMysqlKeyAuthority extends HiveMySqlTestCase{
	DataSource ds = null;

	@Test
	@SuppressWarnings("unchecked")
	public void testAssign() throws Exception {
		KeyAuthority authority = new MySqlKeyAuthority(
				getDataSource("test"), this.getClass(), Integer.class);
		int firstKey = (Integer) authority.nextAvailableKey();
		int secondKey = (Integer) authority.nextAvailableKey();
		assertTrue(secondKey > firstKey);
	}
	
	@Override
	public Collection<String> getDatabaseNames() {
		return Arrays.asList(new String[]{getHiveDatabaseName()});
	}

	@Override
	public String getHiveDatabaseName() {
		return "storage_test";
	}
}
